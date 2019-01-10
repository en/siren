package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"

	"github.com/en/siren/messaging"
	log "github.com/en/siren/utils/glog"
)

func initMessaging() {
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":               "REMOVED.us-central1.gcp.confluent.cloud:9092",
		"api.version.request":             true,
		"broker.version.fallback":         "0.10.0.0",
		"api.version.fallback.ms":         0,
		"sasl.mechanisms":                 "PLAIN",
		"security.protocol":               "SASL_SSL",
		"ssl.ca.location":                 "/etc/ssl/cert.pem",
		"sasl.username":                   "REMOVED",
		"sasl.password":                   "REMOVED",
		"group.id":                        "siren-sql",
		"session.timeout.ms":              6000,
		"go.events.channel.enable":        true,
		"go.application.rebalance.enable": true,
		// Enable generation of PartitionEOF when the
		// end of a partition is reached.
		"enable.partition.eof": true,
		"auto.offset.reset":    "earliest"})

	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create consumer: %s\n", err)
		os.Exit(1)
	}
	log.Info().Msgf("Created Consumer %v\n", consumer)

	err = consumer.SubscribeTopics([]string{messaging.TopicSirenTransfers}, nil)
	if err != nil {
		log.Fatal().Msgf("Failed to Subscribe topic: %v\n", err)
	}

	ch := make(chan []byte)
	go readMessage(consumer, ch)
	go save(ch)

}

func readMessage(consumer *kafka.Consumer, ch chan []byte) {
	defer func() {
		close(ch)
		fmt.Printf("Closing consumer\n")
		consumer.Close()
	}()
	for {
		select {
		case ev := <-consumer.Events():
			switch e := ev.(type) {
			case kafka.AssignedPartitions:
				fmt.Fprintf(os.Stderr, "%% %v\n", e)
				consumer.Assign(e.Partitions)
			case kafka.RevokedPartitions:
				fmt.Fprintf(os.Stderr, "%% %v\n", e)
				consumer.Unassign()
			case *kafka.Message:
				fmt.Printf("%% Message on %s:\n%s: %s\n",
					e.TopicPartition, string(e.Key), string(e.Value))
				switch *e.TopicPartition.Topic {
				case messaging.TopicSirenTransfers:
					log.Debug().Str("value", string(e.Value)).Msg("receive a siren transfer message")
					ch <- e.Value
				default:
					log.Error().Str("topic", *e.TopicPartition.Topic).Msg("unknown topic")
				}
			case kafka.PartitionEOF:
				fmt.Printf("%% Reached %v\n", e)
			case kafka.Error:
				// Errors should generally be considered as informational, the client will try to automatically recover
				fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
			}
		}
	}
}

func save(ch chan []byte) {
	transfers := make([][]byte, 0)
	timer := time.After(500 * time.Millisecond)
	for {
		select {
		case <-timer:
			timer = time.After(500 * time.Millisecond)
			if len(transfers) == 0 {
				continue
			}
			err := bulkInsert(transfers)
			if err != nil {
				log.Error().Err(err).Msg("failed to bulk insert")
			}
			transfers = make([][]byte, 0)
			log.Debug().Msg("tick ...")
		case value, ok := <-ch:
			if !ok {
				log.Debug().Msg("ch not ok")
				return
			}
			transfers = append(transfers, value)
			log.Debug().Str("new value", string(value)).Msg("new value appended")
		}
	}
}

func bulkInsert(transfers [][]byte) error {
	ctx := context.Background()
	valueStrings := make([]string, 0)
	valueArgs := make([]interface{}, 0)
	i := 0
	for _, t := range transfers {
		var transfer messaging.TransferMessage
		err := json.Unmarshal(t, &transfer)
		if err != nil {
			log.Error().Str("value", string(t)).Msg("failed to unmarshal transfer")
			return err
		}
		if transfer.Status == "done" {
			stmt := "UPDATE transfers SET status = $1 WHERE id = $2"
			log.Debug().Str("stmt", stmt).Msg("update transfers")
			result, err := db.ExecContext(ctx, stmt, transfer.Status, transfer.Id)
			fmt.Println(result, err)
			if err != nil {
				continue
			}
			rows, err := result.RowsAffected()
			fmt.Println(rows, err)
		} else {
			valueStrings = append(valueStrings, fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d)", i*10+1, i*10+2, i*10+3, i*10+4, i*10+5, i*10+6, i*10+7, i*10+8, i*10+9, i*10+10))
			valueArgs = append(valueArgs, transfer.Id)
			valueArgs = append(valueArgs, transfer.UserId)
			valueArgs = append(valueArgs, transfer.Currency)
			valueArgs = append(valueArgs, transfer.Type)
			valueArgs = append(valueArgs, transfer.Amount)
			valueArgs = append(valueArgs, 0.0)
			valueArgs = append(valueArgs, "")
			valueArgs = append(valueArgs, transfer.CreatedAt)
			valueArgs = append(valueArgs, transfer.CreatedAt)
			valueArgs = append(valueArgs, transfer.Status)
			i++
		}
	}
	if len(valueStrings) == 0 {
		return nil
	}
	stmt := fmt.Sprintf("INSERT INTO transfers (id, user_id, currency, type, amount, fee, address, created_at, updated_at, status) VALUES %s", strings.Join(valueStrings, ","))
	log.Debug().Str("stmt", stmt).Msg("insert transfers")
	result, err := db.ExecContext(ctx, stmt, valueArgs...)
	fmt.Println(result, err)
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	fmt.Println(rows, err)
	return err
}
