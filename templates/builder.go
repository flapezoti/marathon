package templates

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"git.topfreegames.com/topfreegames/marathon/messages"

	"github.com/uber-go/zap"
	"github.com/valyala/fasttemplate"
)

type buildError struct {
	Message string
}

func (e buildError) Error() string {
	return fmt.Sprintf("%v", e.Message)
}

// Builder reads the messages from the inChan and generates messages to be sent to Kafka in the outChan
func Builder(inChan <-chan *messages.TemplatedMessage, outChan chan<- *messages.KafkaMessage, doneChan <-chan struct{}) {
	for {
		select {
		case <-doneChan:
			return // breaks out of the for
		case msg := <-inChan:
			message, err := Build(msg)
			if err != nil {
				Logger.Error("Error building message", zap.Error(err))
				continue
			}
			outChan <- message
		}
	}
}

// Replace replaces the template parameters from message with the values of the keys in params.
// the function returns the built string
func Replace(message string, params map[string]interface{}) (string, error) {
	t, err := fasttemplate.NewTemplate(string(message), "{{", "}}")
	if err != nil {
		Logger.Error("Template Error", zap.Error(err))
		return "", err
	}

	s := t.ExecuteFuncString(func(w io.Writer, tag string) (int, error) {
		pieces := strings.Split(tag, ".")
		var item interface{}
		item = params
		if len(pieces) > 1 {
			for _, piece := range pieces {
				switch item.(type) {
				case map[string]interface{}:
					item = item.(map[string]interface{})[piece]
				default:
					return 0, nil
				}
			}
			return w.Write([]byte(fmt.Sprintf("%v", item)))
		}

		if val, ok := params[tag]; ok {
			return w.Write([]byte(fmt.Sprintf("%v", val)))
		}
		return 0, nil
	})
	return s, nil
}

// ApnsMsg builds an apns message
func ApnsMsg(request *messages.TemplatedMessage, content string) (string, error) {
	msg := messages.NewApnsMessage()
	msg.DeviceToken = request.Token
	msg.PushExpiry = request.PushExpiry
	msg.Payload.Aps = json.RawMessage(content)
	if len(request.Metadata) > 0 {
		msg.Payload.M = request.Metadata
	}

	b, err := json.Marshal(msg)
	if err != nil {
		Logger.Error(
			"Error building apns msg",
			zap.String("msg", fmt.Sprintf("%+v", msg)),
			zap.Error(err),
		)
		return "", err
	}
	strMsg := string(b)
	return strMsg, nil
}

// GcmMsg builds a GCM message
func GcmMsg(request *messages.TemplatedMessage, content string) (string, error) {
	msg := messages.NewGcmMessage()
	msg.To = request.Token
	msg.PushExpiry = request.PushExpiry
	err := json.Unmarshal([]byte(content), &msg.Data)
	if err != nil {
		Logger.Error(
			"Error building gcm msg",
			zap.String("msg", fmt.Sprintf("%+v", msg)),
			zap.Error(err),
		)
		return "", err
	}
	if len(request.Metadata) > 0 {
		msg.Data["m"] = request.Metadata
	}

	b, err := json.Marshal(msg)
	if err != nil {
		Logger.Error(
			"Error building gcm msg",
			zap.String("msg", fmt.Sprintf("%+v", msg)),
			zap.Error(err),
		)
		return "", err
	}
	strMsg := string(b)
	return strMsg, nil
}

// Build receives a RequestMessage with Message filled with the message to be built, if Params
// has any content. If the request was a template it should have already been fetched and placed
// into the Message field.
func Build(request *messages.TemplatedMessage) (*messages.KafkaMessage, error) {
	// Replace template
	byteMessage, err := json.Marshal(request.Message)
	if err != nil {
		Logger.Error("Error marshalling message", zap.Error(err))
		return nil, err
	}
	stringMessage := string(byteMessage)
	content, err := Replace(stringMessage, request.Params)
	if err != nil {
		Logger.Error("Template replacing error", zap.Error(err))
		return nil, err
	}

	// FIXME: What is the rigth topic?
	topic := request.App

	var builtMessage string
	if request.Service == "apns" {
		builtMessage, err = ApnsMsg(request, content)
		if err != nil {
			return nil, err
		}
	} else if request.Service == "gcm" {
		builtMessage, err = GcmMsg(request, content)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, buildError{"Unknown request message type"}
	}
	if builtMessage == "" {
		return nil, buildError{"Failed to build KafkaMessage"}
	}

	return &messages.KafkaMessage{Message: builtMessage, Topic: topic}, nil
}