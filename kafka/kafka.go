/*
http://www.apache.org/licenses/LICENSE-2.0.txt


Copyright 2015 Intel Corporation

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package kafka

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/intelsdi-x/snap/control/plugin"
	"github.com/intelsdi-x/snap/control/plugin/cpolicy"
	"github.com/intelsdi-x/snap/core/ctypes"

	"crypto/tls"
	log "github.com/intelsdi-x/snap-plugin-utilities/logger"
	"gopkg.in/Shopify/sarama.v1"
)

const (
	PluginName    = "kafka"
	PluginVersion = 9
	PluginType    = plugin.PublisherPluginType
)

func Meta() *plugin.PluginMeta {
	return plugin.NewPluginMeta(PluginName, PluginVersion, PluginType, []string{plugin.SnapGOBContentType}, []string{plugin.SnapGOBContentType})
}

type kafkaPublisher struct{}

func NewKafkaPublisher() *kafkaPublisher {
	return &kafkaPublisher{}
}

type MetricToPublish struct {
	// The timestamp from when the metric was created.
	Timestamp time.Time         `json:"timestamp"`
	Namespace string            `json:"namespace"`
	Data      interface{}       `json:"data"`
	Unit      string            `json:"unit"`
	Tags      map[string]string `json:"tags"`
	Version_  int               `json:"version"`
	// Last advertised time is the last time the snap agent was told about a metric.
	LastAdvertisedTime time.Time `json:"last_advertised_time"`
}

// Publish sends data to a Kafka server
func (k *kafkaPublisher) Publish(contentType string, content []byte, config map[string]ctypes.ConfigValue) error {
	var mts []plugin.MetricType

	switch contentType {
	case plugin.SnapGOBContentType:
		dec := gob.NewDecoder(bytes.NewBuffer(content))
		// decode incoming metrics types
		if err := dec.Decode(&mts); err != nil {
			fmt.Fprintf(os.Stderr, "Error: invalid incoming content: %v, err=%v", content, err)
			return fmt.Errorf("Cannot decode incoming content, err=%v", err)
		}
	default:
		return fmt.Errorf("Unknown content type '%s'", contentType)
	}
	// format metrics types to metrics to be published
	metrics := formatMetricTypes(mts)

	jsonOut, err := json.Marshal(metrics)
	if err != nil {
		return fmt.Errorf("Cannot marshal metrics to JSON format, err=%v", err)
	}

	topic := config["topic"].(ctypes.ConfigValueStr).Value
	brokers := parseBrokerString(config["brokers"].(ctypes.ConfigValueStr).Value)

	// Check if tls_certificate key exists in config map
	if val, ok := config["tls_certificate"]; ok {
		if val.(ctypes.ConfigValueStr).Value != "" {
			log.LogInfo("TLS certificate defined within SNAP configuration. Enabling TLS.")

			tlsConfig := getTlsConfig(config)

			return k.publish(topic, brokers, []byte(jsonOut), tlsConfig)
		} else {
			log.LogFatal("Please provide file path to location of your signed TLS certificate.")
		}

	}

	return k.publish(topic, brokers, []byte(jsonOut), nil)
}

func (k *kafkaPublisher) GetConfigPolicy() (*cpolicy.ConfigPolicy, error) {
	cp := cpolicy.New()
	config := cpolicy.NewPolicyNode()

	r1, err := cpolicy.NewStringRule("topic", false, "snap")
	handleErr(err)
	r1.Description = "Kafka topic for publishing"

	r2, _ := cpolicy.NewStringRule("brokers", false, "localhost:9092")
	handleErr(err)
	r2.Description = "List of brokers separated by semicolon in the format: <broker-ip:port;broker-ip:port> (ex: \"192.168.1.1:9092;172.16.9.99:9092\")"

	config.Add(r1, r2)
	cp.Add([]string{""}, config)
	return cp, nil
}

// Internal method after data has been converted to serialized bytes to send
func (k *kafkaPublisher) publish(topic string, brokers []string, content []byte, tlsConfig *sarama.Config) error {
	producer, err := sarama.NewSyncProducer(brokers, tlsConfig)
	if err != nil {
		return fmt.Errorf("Cannot initialize a new Sarama SyncProducer using the given broker addresses (%v), err=%v", brokers, err)
	}

	defer func() {
		if err := producer.Close(); err != nil {
			panic(err)
		}
	}()

	_, _, err = producer.SendMessage(&sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(content),
	})
	return err
}

// formatMetricTypes returns metrics in format to be publish as a JSON based on incoming metrics types;
// i.a. namespace is formatted as a single string
func formatMetricTypes(mts []plugin.MetricType) []MetricToPublish {
	var metrics []MetricToPublish
	for _, mt := range mts {
		metrics = append(metrics, MetricToPublish{
			Timestamp:          mt.Timestamp(),
			Namespace:          mt.Namespace().String(),
			Data:               mt.Data(),
			Unit:               mt.Unit(),
			Tags:               mt.Tags(),
			Version_:           mt.Version(),
			LastAdvertisedTime: mt.LastAdvertisedTime(),
		})
	}
	return metrics
}
func parseBrokerString(brokerStr string) []string {
	// remove spaces from 'brokerStr'
	brokers := strings.Replace(brokerStr, " ", "", -1)

	// return split brokers separated by semicolon
	return strings.Split(brokers, ";")
}

func getTlsConfig(config map[string]ctypes.ConfigValue) *sarama.Config {
	kconfig := sarama.NewConfig()
	kconfig.Net.TLS.Enable = true

	cert, err := tls.LoadX509KeyPair(config["tls_certificate"].(ctypes.ConfigValueStr).Value,
		config["tls_key"].(ctypes.ConfigValueStr).Value)
	if err != nil {
		panic(err)
	}

	tlsCfg := &tls.Config{Certificates: []tls.Certificate{cert}, InsecureSkipVerify: true}

	kconfig.Net.TLS.Config = tlsCfg

	return kconfig
}

func handleErr(e error) {
	if e != nil {
		panic(e)
	}
}
