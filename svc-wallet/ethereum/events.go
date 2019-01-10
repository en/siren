package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/params"
	"github.com/gofrs/uuid"
	"github.com/shopspring/decimal"

	"github.com/en/siren/messaging"
	log "github.com/en/siren/utils/glog"
)

var Confirmations = big.NewInt(30)

// block number -> transfer_id
var pendingTxs map[string][]string

var Ether, _ = decimal.NewFromString(big.NewInt(params.Ether).String())

func loop(client *ethclient.Client) {
	heads := make(chan *types.Header, 16)
	sub, err := client.SubscribeNewHead(context.Background(), heads)
	if err != nil {
		log.Fatal().Msgf("Failed to subscribe to head events: %v", err)
	}
	defer sub.Unsubscribe()

	for {
		select {
		case head := <-heads:
			timestamp := time.Unix(head.Time.Int64(), 0)
			log.Debug().Interface("time", timestamp)
			if time.Since(timestamp) > time.Hour {
				log.Warn().Interface("number", head.Number).Interface("hash", head.Hash()).Interface("age", timestamp).Msg("Skipping faucet refresh, head too old")
				continue
			}
			log.Info().Interface("number", head.Number).Interface("hash", head.Hash()).Interface("age", timestamp).Msg("Updated faucet state")
			_, err := client.BlockByHash(context.Background(), head.Hash())
			if err != nil {
				log.Debug().Err(err).Msg("BlockByHash")
			}
			block, err := client.BlockByNumber(context.Background(), head.Number)
			if err != nil {
				log.Debug().Err(err).Msg("1")
				continue
			}

			completed := new(big.Int).Sub(head.Number, Confirmations)
			fmt.Printf("pending_txs: %v\n", pendingTxs)
			fmt.Printf("head: %v, completed: %v\n", head.Number, completed)
			if txs, ok := pendingTxs[completed.String()]; ok {
				for _, tx := range txs {
					transfer := messaging.TransferMessage{
						Id:       tx,
						Currency: "ETH",
						Type:     "Deposit",
						Status:   "done",
					}
					b, err := json.Marshal(transfer)
					if err != nil {
						log.Error().Err(err).Msg("failed to marshal kafka transfer message")
						continue
					}
					err = producer.Produce(&kafka.Message{
						TopicPartition: kafka.TopicPartition{Topic: &messaging.TopicSirenTransfers, Partition: kafka.PartitionAny},
						Value:          b,
						// Headers:        []kafka.Header{{Key: "myTestHeader", Value: []byte("header values are binary")}},
					}, nil)
				}
				delete(pendingTxs, completed.String())
			}
			for _, tx := range block.Transactions() {
				fmt.Println("tx_hash", tx.Hash().Hex()) // 0xREMOVED
				fmt.Println(tx.Value().String())        // 10000000000000000
				fmt.Println(tx.Gas())                   // 105000
				fmt.Println(tx.GasPrice().Uint64())     // 102000000000
				fmt.Println(tx.Nonce())                 // 110644
				fmt.Println(tx.Data())                  // []
				if tx.To() != nil {
					fmt.Println("to:", tx.To().Hex()) // 0xREMOVED
				} else {
					log.Warn().Msg("to is nil")
				}

				chainID, err := client.NetworkID(context.Background())
				if err != nil {
					log.Fatal().Err(err).Msg("")
				}

				if msg, err := tx.AsMessage(types.NewEIP155Signer(chainID)); err == nil {
					fmt.Println("from:", msg.From().Hex()) // 0xREMOVED
				}

				receipt, err := client.TransactionReceipt(context.Background(), tx.Hash())
				if err != nil {
					log.Fatal().Err(err).Msg("2")
				}

				fmt.Println(receipt.Status) // 1
				if tx.Value().Cmp(new(big.Int)) == 1 {
					if info, ok := addresses[tx.To().Hex()]; ok {
						id, err := uuid.NewV4()
						if err != nil {
							log.Error().Err(err).Msg("failed to generate UUID")
							continue
						}
						value, err := decimal.NewFromString(tx.Value().String())
						if err != nil {
							log.Error().Err(err).Msg("wrong decimal")
							continue
						}
						amount := value.Div(Ether)
						transfer := messaging.TransferMessage{
							Id:        id.String(),
							UserId:    info.userId,
							Currency:  "ETH",
							Type:      "Deposit",
							Amount:    amount,
							Status:    "pending",
							CreatedAt: time.Now(),
						}
						b, err := json.Marshal(transfer)
						if err != nil {
							log.Error().Err(err).Msg("failed to marshal kafka transfer message")
							continue
						}
						err = producer.Produce(&kafka.Message{
							TopicPartition: kafka.TopicPartition{Topic: &messaging.TopicSirenTransfers, Partition: kafka.PartitionAny},
							Value:          b,
							// Headers:        []kafka.Header{{Key: "myTestHeader", Value: []byte("header values are binary")}},
						}, nil)
						if pendingTxs[head.Number.String()] == nil {
							pendingTxs[head.Number.String()] = make([]string, 0)
						}
						pendingTxs[head.Number.String()] = append(pendingTxs[head.Number.String()], id.String())
					}
				}
			}

		}
	}
}
