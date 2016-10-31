package consumer

import (
	"fmt"

	"git.topfreegames.com/topfreegames/marathon/extensions"
	"git.topfreegames.com/topfreegames/marathon/log"
	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"github.com/spf13/viper"
	"github.com/uber-go/zap"
)

// FIXME: Try to use a better way to define log level (do the same in the entire project)

// consumeError is an error generated during data consumption
type consumeError struct {
	Message string
}

func (e consumeError) Error() string {
	return fmt.Sprintf("%v", e.Message)
}

// Consumer reads from the specified Kafka topic while the Messages channel is open
func Consumer(l zap.Logger, config *viper.Viper, app, service string, outChan chan<- string, doneChan <-chan struct{}) error {
	// Set configurations for consumer
	clusterConfig := cluster.NewConfig()
	clusterConfig.Consumer.Return.Errors = true
	clusterConfig.Group.Return.Notifications = true
	clusterConfig.Version = sarama.V0_9_0_0
	clusterConfig.Consumer.Offsets.Initial = sarama.OffsetOldest

	zkClient := extensions.GetZkClient(config.ConfigFileUsed())

	brokers, err := zkClient.GetKafkaBrokers()

	if err != nil {
		panic(err)
	}

	consumerGroupTemlate := config.GetString("workers.consumer.consumergroupTemplate")
	topicTemplate := config.GetString("workers.consumer.topicTemplate")
	topics := []string{fmt.Sprintf(topicTemplate, app, service)}
	consumerGroup := fmt.Sprintf(consumerGroupTemlate, app, service)

	l = l.With(
		zap.Object("brokers", brokers),
		zap.String("consumerGroupTemlate", consumerGroupTemlate),
		zap.Object("consumerGroup", consumerGroup),
		zap.String("topicTemplate", topicTemplate),
		zap.Object("topics", topics),
		zap.String("clusterConfig", fmt.Sprintf("%+v", clusterConfig)),
	)

	// Create consumer defined by the configurations
	consumer, err := cluster.NewConsumer(brokers, consumerGroup, topics, clusterConfig)
	if err != nil {
		log.E(l, "Could not create consumer", func(cm log.CM) {
			cm.Write(zap.String("error", err.Error()))
		})
		return err
	}
	// FIXME: When we should close it
	// defer consumer.Close()
	log.D(l, "Created consumer", func(cm log.CM) {
		cm.Write(zap.String("consumer", fmt.Sprintf("%+v", consumer)))
	})

	go func() {
		for err := range consumer.Errors() {
			log.E(l, "Consumer error", func(cm log.CM) {
				cm.Write(zap.String("error", err.Error()))
			})
		}
	}()

	go func() {
		for notif := range consumer.Notifications() {
			log.I(l, "Rebalanced", func(cm log.CM) {
				cm.Write(zap.Object("", notif))
			})
		}
	}()
	log.I(l, "Starting kafka consumer")
	MainLoop(l, consumer, outChan, doneChan)
	log.I(l, "Stopped kafka consumer")
	return nil
}

// MainLoop to read messages from Kafka and send them forward in the pipeline
func MainLoop(l zap.Logger, consumer *cluster.Consumer, outChan chan<- string, doneChan <-chan struct{}) {
	for {
		select {
		case <-doneChan:
			return // breaks out of the for
		case msg, ok := <-consumer.Messages():
			if !ok {
				log.E(l, "Not ok consuming from Kafka", func(cm log.CM) {
					cm.Write(zap.Object("msg", msg))
				})
				return // breaks out of the for
			}
			strMsg, err := Consume(l, msg)
			if err != nil {
				log.E(l, "Error reading kafka message", func(cm log.CM) {
					cm.Write(zap.Error(err))
				})
				continue
			}

			log.D(l, "Consumed message", func(cm log.CM) {
				cm.Write(
					zap.String("message", strMsg),
					zap.String("consumer", fmt.Sprintf("%+v", consumer)),
				)
			})
			// FIXME: Is it the rigth place to mark offset?
			consumer.MarkOffset(msg, "")
			outChan <- strMsg
		}
	}
}

// Consume extracts the message from the consumer message
func Consume(l zap.Logger, kafkaMsg *sarama.ConsumerMessage) (string, error) {
	log.I(l, "Consume message", func(cm log.CM) {
		cm.Write(zap.Object("msg", kafkaMsg))
	})
	msg := string(kafkaMsg.Value)
	if msg == "" {
		return "", consumeError{"Empty message"}
	}
	log.I(l, "Consumed message", func(cm log.CM) {
		cm.Write(zap.String("msg", msg))
	})
	return msg, nil
}
