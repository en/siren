package main

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/gofrs/uuid"
	_ "github.com/lib/pq"
	"github.com/shopspring/decimal"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	pb "github.com/en/siren/protos"
	log "github.com/en/siren/utils/glog"
)

const (
	port = ":50051"
)

var db *sql.DB
var bitcoinClient pb.BitcoinClient
var ethereumClient pb.EthereumClient

type server struct{}

func (s *server) GetAddress(ctx context.Context, in *pb.GetAddressRequest) (*pb.GetAddressResponse, error) {
	userId := in.GetUserId()
	currency := in.GetCurrency()
	log.Info().Str("user_id", userId).Str("currency", currency).Msg("get address")
	response := new(pb.GetAddressResponse)
	var err error
	switch currency {
	case "BTC":
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		response, err = bitcoinClient.GetAddress(ctx, in)
		if err != nil {
			log.Error().Err(err).Msg("failed to generate new BTC address")
			return response, err
		}
	case "ETH":
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		response, err = ethereumClient.GetAddress(ctx, in)
		if err != nil {
			log.Error().Err(err).Msg("failed to generate new ETH address")
			return response, err
		}
	default:
		log.Error().Str("currency", currency).Msg("unknown currency")
		// TODO: return error
		return response, nil
	}
	return response, nil
}

func (s *server) Withdraw(ctx context.Context, in *pb.WithdrawRequest) (*pb.WithdrawResponse, error) {
	response := new(pb.WithdrawResponse)
	userId := in.GetUserId()
	currency := in.GetCurrency()
	address := in.GetAddress()
	log.Info().Str("user_id", userId).Str("currency", currency).Str("amount", in.GetAmount()).Msg("withdraw request")
	amount, err := decimal.NewFromString(in.GetAmount())
	if err != nil {
		return response, err
	}
	fee := decimal.NewFromFloat(0.0)
	now := time.Now()
	id, err := uuid.NewV4()
	if err != nil {
		log.Error().Err(err).Msg("failed to generate UUID")
		return response, err
	}
	result, err := db.ExecContext(ctx, "INSERT INTO transfers (id, user_id, currency, type, amount, fee, address, created_at, updated_at, status) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)", id, userId, currency, "Withdrawal", amount, fee, address, now, now, "pending")
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
	return response, nil
}

func main() {
	log.Info().Msg("Starting siren-wallet")

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

	bitcoinAddr := viper.GetString("bitcoin")
	conn, err := grpc.Dial(bitcoinAddr, grpc.WithInsecure())
	if err != nil {
		log.Fatal().Err(err).Msg("did not connect")
	}
	defer conn.Close()
	bitcoinClient = pb.NewBitcoinClient(conn)
	log.Info().Str("addr", bitcoinAddr).Msg("connected to bitcoin")

	ethereumAddr := viper.GetString("ethereum")
	conn, err = grpc.Dial(ethereumAddr, grpc.WithInsecure())
	if err != nil {
		log.Fatal().Err(err).Msg("did not connect")
	}
	defer conn.Close()
	ethereumClient = pb.NewEthereumClient(conn)
	log.Info().Str("addr", ethereumAddr).Msg("connected to ethereum")

	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to listen")
	}
	s := grpc.NewServer()
	pb.RegisterWalletServer(s, &server{})
	reflection.Register(s)
	if err := s.Serve(lis); err != nil {
		log.Fatal().Err(err).Msg("failed to serve")
	}
}
