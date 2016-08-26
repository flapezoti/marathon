package consumer

import (
	"fmt"

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

	brokers := config.GetStringSlice("workers.consumer.brokers")
	consumerGroupTemlate := config.GetString("workers.consumer.consumergroupTemplate")
	topicTemplate := config.GetString("workers.consumer.topicTemplate")
	topics := []string{fmt.Sprintf(topicTemplate, app, service)}
	consumerGroup := fmt.Sprintf(consumerGroupTemlate, app, service)
	l.Debug(
		"Create consumer group",
		zap.Object("brokers", brokers),
		zap.String("consumerGroupTemlate", consumerGroupTemlate),
		zap.String("consumerGroup", consumerGroup),
		zap.String("topicTemplate", topicTemplate),
		zap.Object("topics", topics),
		zap.Object("clusterConfig", clusterConfig),
	)

	// Create consumer defined by the configurations
	consumer, err := cluster.NewConsumer(brokers, consumerGroup, topics, clusterConfig)
	if err != nil {
		l.Error("Could not create consumer", zap.String("error", err.Error()))
		return err
	}
	// FIXME: When we should close it
	// defer consumer.Close()
	l.Debug(
		"Created consumer",
		zap.Object("brokers", brokers),
		zap.Object("consumerGroup", consumerGroup),
		zap.Object("topics", topics),
		zap.String("clusterConfig", fmt.Sprintf("%+v", clusterConfig)),
		zap.String("consumer", fmt.Sprintf("%+v", consumer)),
	)

	go func() {
		for err := range consumer.Errors() {
			l.Error("Consumer error", zap.String("error", err.Error()))
		}
	}()

	go func() {
		for notif := range consumer.Notifications() {
			l.Info("Rebalanced", zap.Object("", notif))
		}
	}()
	l.Info("Starting kafka consumer")
	MainLoop(l, consumer, outChan, doneChan)
	l.Info("Stopped kafka consumer")
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
				l.Error("Not ok consuming from Kafka", zap.Object("msg", msg))
				return // breaks out of the for
			}
			strMsg, err := Consume(l, msg)
			if err != nil {
				l.Error("Error reading kafka message", zap.Error(err))
				continue
			}

			l.Debug(
				"Consumed message",
				zap.String("message", strMsg),
				zap.String("consumer", fmt.Sprintf("%+v", consumer)),
			)
			// FIXME: Is it the rigth place to mark offset?
			consumer.MarkOffset(msg, "")
			outChan <- strMsg
		}
	}
}

// Consume extracts the message from the consumer message
func Consume(l zap.Logger, kafkaMsg *sarama.ConsumerMessage) (string, error) {
	l.Info("Consume message", zap.Object("msg", kafkaMsg))
	msg := string(kafkaMsg.Value)
	if msg == "" {
		return "", consumeError{"Empty message"}
	}
	l.Info("Consumed message", zap.String("msg", msg))
	return msg, nil
}
