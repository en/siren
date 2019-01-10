package main

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/ethclient"
	_ "github.com/lib/pq"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	pb "github.com/en/siren/protos"
	log "github.com/en/siren/utils/glog"
)

const (
	port = ":50053"
)

var db *sql.DB

type addressInfo struct {
	userId       string
	accountIndex uint32
}

var addresses map[string]addressInfo

type server struct{}

func (s *server) GetAddress(ctx context.Context, in *pb.GetAddressRequest) (*pb.GetAddressResponse, error) {
	response := new(pb.GetAddressResponse)
	var address string
	var addressSig string
	err := db.QueryRowContext(ctx, "SELECT address, address_sig FROM deposit_addresses_eth WHERE user_id = $1", in.UserId).Scan(&address, &addressSig)
	switch {
	case err == sql.ErrNoRows:
		log.Printf("No address with user_id %s", in.UserId)
		address, err = getAddress(accountIndex)
		now := time.Now()
		result, err := db.ExecContext(ctx, "INSERT INTO deposit_addresses_eth (address, address_sig, user_id, created_at, updated_at, status) VALUES ($1, $2, $3, $4, $5, $6)", address, "", in.UserId, now, now, "")
		if err != nil {
			log.Error().Err(err).Msg("insert error")
			return response, err
		}
		rows, err := result.RowsAffected()
		if err != nil {
			log.Error().Err(err).Msg("rows affected error")
			return response, err
		}
		if rows != 1 {
			log.Error().Int64("rows", rows).Msg("rows != 1")
			return response, err
		}
		// TODO: atomic
		accountIndex = accountIndex + 1
	case err != nil:
		log.Fatal().Err(err).Msg("")
		return response, err
	}
	response.Address = address
	return response, nil
}

func main() {
	log.Info().Msg("Starting siren-wallet ethereum")

	viper.SetEnvPrefix("wallet")
	viper.AutomaticEnv()
	replacer := strings.NewReplacer("-", "_")
	viper.SetEnvKeyReplacer(replacer)

	pqInfo := fmt.Sprintf("user=%s password=%s host=%s port=5432 dbname=%s sslmode=disable",
		viper.GetString("db-user"),
		viper.GetString("db-password"),
		viper.GetString("db-host"),
		viper.GetString("db-dbname"))
	var err error
	db, err = sql.Open("postgres", pqInfo)
	if err != nil {
		log.Fatal().Msgf("failed to open database: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		log.Fatal().Msgf("failed to ping: %v", err)
	}

	// init account index
	err = db.QueryRowContext(ctx, "SELECT id FROM deposit_addresses_eth ORDER BY id DESC LIMIT 1").Scan(&accountIndex)
	switch {
	case err == sql.ErrNoRows:
		log.Info().Msg("Maybe we have a brand new database, set account_index to 1")
		accountIndex = 1
	case err != nil:
		log.Fatal().Err(err).Msg("")
	default:
		accountIndex = accountIndex + 1
		log.Info().Msgf("Set initial account_index to %d", accountIndex)
	}
	// init addresses map
	addresses = make(map[string]addressInfo)
	rows, err := db.QueryContext(ctx, "SELECT id, address, user_id FROM deposit_addresses_eth")
	if err != nil {
		log.Fatal().Err(err).Msg("")
	}
	defer rows.Close()

	for rows.Next() {
		var address string
		var ai addressInfo
		if err := rows.Scan(&ai.accountIndex, &address, &ai.userId); err != nil {
			log.Fatal().Err(err).Msg("")
		}

		addresses[address] = ai
	}
	if err := rows.Err(); err != nil {
		log.Fatal().Err(err).Msg("")
	}
	fmt.Println(addresses)
	pendingTxs = make(map[string][]string)
	// TODO: init pending list, check head number, clean

	err = initHDWallet()
	if err != nil {
		log.Fatal().Msgf("Failed to init HD Wallet: %v", err)
	}

	client, err := ethclient.Dial("ws://kovan:8546")
	if err != nil {
		log.Fatal().Msgf("Failed to connect to URL %v", err)
	}

	initMessaging()
	go loop(client)
	go transferFunds(client)

	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to listen")
	}
	s := grpc.NewServer()
	// TODO
	pb.RegisterEthereumServer(s, &server{})
	reflection.Register(s)
	if err := s.Serve(lis); err != nil {
		log.Fatal().Err(err).Msg("failed to serve")
	}
}
