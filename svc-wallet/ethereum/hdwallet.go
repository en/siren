package main

import (
	"fmt"

	"github.com/miguelmota/go-ethereum-hdwallet"

	log "github.com/en/siren/utils/glog"
)

const (
	HDPathFormat = "m/44'/60'/0'/0/%d"
)

var (
	wallet       *hdwallet.Wallet
	accountIndex uint32
)

func initHDWallet() (err error) {
	// TODO: secret
	mnemonic := "REMOVED REMOVED REMOVED REMOVED REMOVED REMOVED REMOVED REMOVED REMOVED REMOVED REMOVED REMOVED"
	wallet, err = hdwallet.NewFromMnemonic(mnemonic)
	return
}

func getAddress(accountIndex uint32) (string, error) {
	path := hdwallet.MustParseDerivationPath(fmt.Sprintf(HDPathFormat, accountIndex))
	account, err := wallet.Derive(path, false)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to derive account")
		return "", err
	}

	return account.Address.Hex(), nil
}
