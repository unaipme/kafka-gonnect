package connectors

import (
	"github.com/eclipse/paho.mqtt.golang"
	"errors"
	"fmt"
	"math/rand"
	"github.com/segmentio/kafka-go"
	"strings"
	"context"
)

type (
	MqttConnector struct {
		connector
		SourceConnector `json:"-"`
		Client      mqtt.Client   `json:"-"`
		Topic		string        `json:"topic"`
		Qos         byte	      `json:"qos"`
		ClientId    string	      `json:"clientId"`
		kafkaWriter *kafka.Writer `json:"-"`
	}
)

func validateAndParseMqttOptions(conf map[string]interface{}) (*mqtt.ClientOptions, error) {
	if conf["topic"] == nil {
		return nil, errors.New("field 'topic' is required")
	} else if conf["broker"] == nil {
		return nil, errors.New("field 'broker' is required")
	}
	opts := mqtt.NewClientOptions().AddBroker(conf["broker"].(string))
	if conf["client-id"] != nil {
		opts.SetClientID(conf["client-id"].(string))
	}
	return opts, nil
}

func CreateMqttConnection(id, bootstrapServers, topic string, conf map[string]interface{}) (*MqttConnector, error) {
	var ok bool
	conn := MqttConnector{}

	opts, err := validateAndParseMqttOptions(conf)
	if err != nil {
		return nil, err
	}
	if conn.ClientId, ok = conf["client-id"].(string); !ok {
		opts.SetClientID(fmt.Sprintf("mqtt-%x", rand.Uint32()))
	}
	opts.SetDefaultPublishHandler(func (client mqtt.Client, message mqtt.Message) {
		fmt.Printf("TOPIC: %s\nMESSAGE: %s\n\n", message.Topic(), message.Payload())
		conn.kafkaWriter.WriteMessages(context.Background(), kafka.Message{
			Key: []byte(message.Topic()),
			Value: []byte(message.Payload()),
		})
	})

	c := mqtt.NewClient(opts)
	if token := c.Connect(); token.Wait() && token.Error() != nil {
		return nil, token.Error()
	}

	conn.Id = id
	conn.Type = conn.GetType()
	conn.Topic = conf["topic"].(string)
	conn.Client = c
	if conn.Qos, ok = conf["qos"].(byte); !ok {
		conn.Qos = 0
	}

	if err = conn.subscribe(); err != nil {
		return nil, err
	}

	conn.kafkaWriter = kafka.NewWriter(kafka.WriterConfig{
		Brokers: strings.Split(bootstrapServers, ","),
		Topic: topic,
		Balancer: &kafka.LeastBytes{},
	})

	return &conn, nil
}

func (client MqttConnector) subscribe() (error) {
	token := client.Client.Subscribe(client.Topic, client.Qos, nil)
	token.Wait()
	return token.Error()
}

func (client MqttConnector) Close() {
	client.Client.Disconnect(250)
}

func (client MqttConnector) Publish(message interface{}) (error) {
	token := client.Client.Publish(client.Topic, client.Qos, false, message)
	token.Wait()
	return token.Error()
}

func (client MqttConnector) GetId() (string) {
	return client.Id
}

func (client MqttConnector) GetType() (string) {
	return "mqttConnector"
}