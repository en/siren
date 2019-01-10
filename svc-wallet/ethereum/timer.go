package main

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/miguelmota/go-ethereum-hdwallet"

	log "github.com/en/siren/utils/glog"
)

var toAddress = common.HexToAddress("0xREMOVED")

func transferFunds(client *ethclient.Client) {
	timer := time.After(1 * time.Hour)
	for {
		select {
		case <-timer:
			timer = time.After(1 * time.Hour)
			for k, v := range addresses {
				account := common.HexToAddress(k)
				balance, err := client.BalanceAt(context.Background(), account, nil)
				if err != nil {
					log.Fatal().Err(err).Msg("")
					continue
				}
				if balance.Cmp(new(big.Int)) == 1 {
					nonce := uint64(0)
					gasLimit := uint64(21000)
					gasPrice := big.NewInt(21000000000)
					var data []byte

					tx := types.NewTransaction(nonce, toAddress, balance, gasLimit, gasPrice, data)

					path := hdwallet.MustParseDerivationPath(fmt.Sprintf(HDPathFormat, v.accountIndex))
					account, err := wallet.Derive(path, false)
					if err != nil {
						log.Fatal().Err(err).Msg("")
						continue
					}
					signedTx, err := wallet.SignTx(account, tx, nil)
					if err != nil {
						log.Fatal().Err(err).Msg("")
						continue
					}
					err = client.SendTransaction(context.Background(), signedTx)
					if err != nil {
						log.Fatal().Err(err).Msg("")
						continue
					}

					fmt.Printf("tx sent: %s", signedTx.Hash().Hex())
				}
			}
		}
	}
}
