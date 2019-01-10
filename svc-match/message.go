package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/davecgh/go-spew/spew"
	"github.com/shopspring/decimal"

	log "github.com/en/siren/utils/glog"
)

var (
	topicBalances      = "balances"
	topicOrders        = "orders"
	topicDeals         = "deals"
	topicSirenBalances = "siren-balances-v1"
	topicSirenOrders   = "siren-orders-v1"
)

type OrderEventType uint32

const (
	ORDER_EVENT_TYPE_PUT    OrderEventType = 1
	ORDER_EVENT_TYPE_UPDATE OrderEventType = 2
	ORDER_EVENT_TYPE_FINISH OrderEventType = 3
)

type Kb struct {
	RefId     string `json:"ref_id"`
	Type      string `json:"type"`
	Available string `json:"available"`
	Hold      string `json:"hold"`
}

var producer *kafka.Producer

func loadFromKafka() {
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":       "REMOVED.us-central1.gcp.confluent.cloud:9092",
		"api.version.request":     true,
		"broker.version.fallback": "0.10.0.0",
		"api.version.fallback.ms": 0,
		"sasl.mechanisms":         "PLAIN",
		"security.protocol":       "SASL_SSL",
		"ssl.ca.location":         "/etc/ssl/cert.pem",
		"sasl.username":           "REMOVED",
		"sasl.password":           "REMOVED",
		"group.id":                "siren-match",
		"session.timeout.ms":      6000,
		"enable.partition.eof":    true,
		"auto.offset.reset":       "earliest"})

	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create consumer: %s\n", err)
		os.Exit(1)
	}

	log.Info().Msgf("Created Consumer %v\n", consumer)

	log.Info().Msg("Loading balances")
	err = consumer.SubscribeTopics([]string{topicSirenBalances}, func(c *kafka.Consumer, event kafka.Event) error {
		log.Info().Msgf("Rebalanced: %s", event)
		switch e := event.(type) {
		case kafka.AssignedPartitions:
			for i := range e.Partitions {
				log.Debug().Str("partition", spew.Sdump(e.Partitions[i])).Msg("reset offset of partition")
				e.Partitions[i].Offset = kafka.OffsetBeginning
			}
			fmt.Fprintf(os.Stderr, "%% %v\n", e)
			consumer.Assign(e.Partitions)
		case kafka.RevokedPartitions:
			fmt.Fprintf(os.Stderr, "%% %v\n", e)
			consumer.Unassign()
		}
		return nil
	})
	run := true

	balances := make(map[string][]byte)

	for run == true {
		ev := consumer.Poll(100)
		if ev == nil {
			continue
		}

		switch e := ev.(type) {
		case *kafka.Message:
			fmt.Printf("%% Message on %s:\n%s: %s\n",
				e.TopicPartition, string(e.Key), string(e.Value))
			if e.Headers != nil {
				fmt.Printf("%% Headers: %v\n", e.Headers)
			}
			switch *e.TopicPartition.Topic {
			case topicSirenBalances:
				log.Debug().Str("key", string(e.Key)).Str("value", string(e.Value)).Msg("receive a siren balances message")
				if e.Value == nil || len(e.Value) == 0 {
					log.Debug().Interface("empty e.Value", e.Value).Msg("")
					delete(balances, string(e.Key))
				} else {
					b, ok := balances[string(e.Key)]
					if ok {
						log.Warn().Str("key", string(e.Key)).Str("old value", string(b)).Str("new value", string(e.Value)).Msg("dup balances key")
					}
					balances[string(e.Key)] = e.Value
				}
			default:
				log.Error().Str("topic", *e.TopicPartition.Topic).Msg("unknown topic")
			}
		case kafka.Error:
			// Errors should generally be considered as informational, the client will try to automatically recover
			fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
		case kafka.PartitionEOF:
			fmt.Printf("%% Reached %v\n", e)
			run = false
		default:
			fmt.Printf("Ignored %v\n", e)
		}
	}

	log.Debug().Msgf("len(balances): %d", len(balances))
	for k, v := range balances {
		keys := strings.Split(k, ":")
		userId, currencyId := keys[0], keys[2]
		var value Kb
		err := json.Unmarshal(v, &value)
		if err != nil {
			log.Error().Str("value", string(v)).Msg("failed to unmarshal value")
			continue
		}

		balanceSet(currencyId, userId, ACCOUNT_TYPE_CASH, value.Available, value.Hold)
	}

	log.Info().Msg("Loading orders")
	err = consumer.SubscribeTopics([]string{topicSirenOrders}, func(c *kafka.Consumer, event kafka.Event) error {
		log.Info().Msgf("Rebalanced: %s", event)
		switch e := event.(type) {
		case kafka.AssignedPartitions:
			for i := range e.Partitions {
				log.Debug().Str("partition", spew.Sdump(e.Partitions[i])).Msg("reset offset of partition")
				e.Partitions[i].Offset = kafka.OffsetBeginning
			}
			fmt.Fprintf(os.Stderr, "%% %v\n", e)
			consumer.Assign(e.Partitions)
		case kafka.RevokedPartitions:
			fmt.Fprintf(os.Stderr, "%% %v\n", e)
			consumer.Unassign()
		}
		return nil
	})
	run = true

	orders := make(map[string][]byte)

	for run == true {
		ev := consumer.Poll(100)
		if ev == nil {
			continue
		}

		switch e := ev.(type) {
		case *kafka.Message:
			fmt.Printf("%% Message on %s:\n%s: %s\n",
				e.TopicPartition, string(e.Key), string(e.Value))
			if e.Headers != nil {
				fmt.Printf("%% Headers: %v\n", e.Headers)
			}
			switch *e.TopicPartition.Topic {
			case topicSirenOrders:
				log.Debug().Str("key", string(e.Key)).Str("value", string(e.Value)).Msg("receive a siren orders message")

				if e.Value == nil || len(e.Value) == 0 {
					log.Debug().Interface("empty e.Value", e.Value).Msg("")
					delete(orders, string(e.Key))
				} else {
					o, ok := orders[string(e.Key)]
					if ok {
						log.Warn().Str("key", string(e.Key)).Str("old value", string(o)).Str("new value", string(e.Value)).Msg("dup orders key")
					}
					orders[string(e.Key)] = e.Value
				}
			default:
				log.Error().Str("topic", *e.TopicPartition.Topic).Msg("unknown topic")
			}
		case kafka.Error:
			// Errors should generally be considered as informational, the client will try to automatically recover
			fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
		case kafka.PartitionEOF:
			fmt.Printf("%% Reached %v\n", e)
			run = false
		default:
			fmt.Printf("Ignored %v\n", e)
		}
	}

	log.Debug().Msgf("len(orders): %d", len(orders))
	for _, v := range orders {
		order := new(order)
		err := json.Unmarshal(v, order)
		if err != nil {
			log.Error().Str("value", string(v)).Msg("failed to unmarshal order")
			continue
		}
		if market, ok := currencyPairs[order.Symbol]; ok {
			orderPut(false, market, order)
		}
	}

	fmt.Printf("Closing consumer\n")
	consumer.Close()
}

func initMessage() {
	var err error
	producer, err = kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":       "REMOVED.us-central1.gcp.confluent.cloud:9092",
		"api.version.request":     true,
		"broker.version.fallback": "0.10.0.0",
		"api.version.fallback.ms": 0,
		"sasl.mechanisms":         "PLAIN",
		"security.protocol":       "SASL_SSL",
		"ssl.ca.location":         "/etc/ssl/cert.pem",
		"sasl.username":           "REMOVED",
		"sasl.password":           "REMOVED",
	})

	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
		os.Exit(1)
	}

	fmt.Printf("Created Producer %v\n", producer)

	go func() {
		for e := range producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				m := ev
				if m.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
				} else {
					fmt.Printf("Delivered message to topic %s [%d] at offset %v\n",
						*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
				}
			default:
				fmt.Printf("Ignored event: %s\n", ev)
			}
		}
	}()
}

func pushBalanceMessage(t int64, userId string, asset string, business string, change decimal.Decimal) {
	var m []interface{}
	m = append(m, t)
	m = append(m, asset)
	m = append(m, userId)
	m = append(m, business)
	m = append(m, change)
	b, err := json.Marshal(m)
	if err != nil {
		log.Error().Err(err).Msg("failed to marshal balance message")
		return
	}
	producer.ProduceChannel() <- &kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &topicBalances, Partition: kafka.PartitionAny}, Value: b}
}

type orderMessage struct {
	Event OrderEventType `json:"event"`
	Order *order         `json:"order"`
	Stock string         `json:"stock"`
	Money string         `json:"money"`
}

func pushOrderMessage(event OrderEventType, o *order, stock string, money string) {
	m := orderMessage{
		Event: event,
		Order: o,
		Stock: stock,
		Money: money,
	}
	b, err := json.Marshal(m)
	if err != nil {
		log.Error().Err(err).Msg("failed to marshal order message")
		return
	}
	producer.ProduceChannel() <- &kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &topicOrders, Partition: kafka.PartitionAny}, Value: b}
}

func pushDealMessage(t int64, market string, ask *order, bid *order, price decimal.Decimal, amount decimal.Decimal, askFee decimal.Decimal, bidFee decimal.Decimal, side int32, id string, stock string, money string) {
	var m []interface{}
	m = append(m, t)
	m = append(m, market)
	m = append(m, ask.Id)
	m = append(m, bid.Id)
	m = append(m, ask.UserId)
	m = append(m, bid.UserId)
	m = append(m, price.String())
	m = append(m, amount.String())
	m = append(m, askFee.String())
	m = append(m, bidFee.String())
	m = append(m, side)
	m = append(m, id)
	m = append(m, stock)
	m = append(m, money)
	b, err := json.Marshal(m)
	if err != nil {
		log.Error().Err(err).Msg("failed to marshal deal message")
		return
	}
	producer.ProduceChannel() <- &kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &topicDeals, Partition: kafka.PartitionAny}, Value: b}
}
