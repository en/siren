package main

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/golang/protobuf/ptypes"
	_ "github.com/lib/pq"
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

type server struct{}

func (s *server) Transfers(ctx context.Context, in *pb.TransfersRequest) (*pb.TransfersResponse, error) {
	response := new(pb.TransfersResponse)
	valueArgs := make([]interface{}, 0)
	i := 1
	stmt := "SELECT id, currency, type, amount, status, created_at, updated_at FROM transfers WHERE user_id = $1"
	valueArgs = append(valueArgs, in.UserId)
	i++

	if in.Currency != "" {
		stmt = stmt + fmt.Sprintf(" AND currency = $%d", i)
		valueArgs = append(valueArgs, in.Currency)
		i++
	}

	if in.Type != "" {
		stmt = stmt + fmt.Sprintf(" AND type = $%d", i)
		valueArgs = append(valueArgs, in.Type)
		i++
	}

	if in.Since != nil {
		since, err := ptypes.Timestamp(in.Since)
		if err == nil {
			stmt = stmt + fmt.Sprintf(" AND created_at >= $%d", i)
			valueArgs = append(valueArgs, since)
			i++
		} else {
			log.Error().Err(err).Msg("")
		}
	}

	if in.Until != nil {
		until, err := ptypes.Timestamp(in.Until)
		if err == nil {
			stmt = stmt + fmt.Sprintf(" AND created_at < $%d", i)
			valueArgs = append(valueArgs, until)
			i++
		} else {
			log.Error().Err(err).Msg("")
		}
	}

	stmt = stmt + " ORDER BY created_at DESC"

	if in.Limit != 0 {
		stmt = stmt + fmt.Sprintf(" LIMIT $%d", i)
		valueArgs = append(valueArgs, in.Limit)
		i++
	}

	log.Debug().Str("stmt", stmt).Interface("args", valueArgs).Msg("query transfers")
	rows, err := db.QueryContext(ctx, stmt, valueArgs...)
	fmt.Println(rows, err)
	if err != nil {
		log.Error().Err(err).Msg("")
		return response, err
	}
	defer rows.Close()

	var createdAt time.Time
	var updatedAt time.Time

	for rows.Next() {
		transfer := new(pb.Transfer)
		if err := rows.Scan(&transfer.Id, &transfer.Currency, &transfer.Type, &transfer.Amount, &transfer.Status, &createdAt, &updatedAt); err != nil {
			log.Error().Err(err).Msg("")
			continue
		}
		ca, err := ptypes.TimestampProto(createdAt)
		if err != nil {
			log.Error().Err(err).Msg("parsing created_at failed")
			continue
		}
		ua, err := ptypes.TimestampProto(updatedAt)
		if err != nil {
			log.Error().Err(err).Msg("parsing updated_at failed")
			continue
		}
		transfer.CreatedAt = ca
		transfer.UpdatedAt = ua
		fmt.Println(transfer)
		response.Transfers = append(response.Transfers, transfer)
	}
	if err := rows.Err(); err != nil {
		log.Error().Err(err).Msg("")
		return response, err
	}
	return response, nil
}

func (s *server) GetOrderHistory(ctx context.Context, in *pb.GetOrderHistoryRequest) (*pb.GetOrderHistoryReply, error) {
	response := new(pb.GetOrderHistoryReply)
	return response, nil
}

func (s *server) GetOrderDeals(ctx context.Context, in *pb.GetOrderDealsRequest) (*pb.GetOrderDealsReply, error) {
	response := new(pb.GetOrderDealsReply)
	return response, nil
}

func (s *server) GetOrderDetailFinished(ctx context.Context, in *pb.GetOrderDetailFinishedRequest) (*pb.GetOrderDetailFinishedReply, error) {
	response := new(pb.GetOrderDetailFinishedReply)
	return response, nil
}

func (s *server) GetMarketUserDeals(ctx context.Context, in *pb.GetMarketUserDealsRequest) (*pb.GetMarketUserDealsReply, error) {
	response := new(pb.GetMarketUserDealsReply)
	return response, nil
}

func main() {
	log.Info().Msg("Starting siren-sql")

	viper.SetEnvPrefix("sql")
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

	initMessaging()

	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to listen")
	}
	s := grpc.NewServer()
	// TODO
	pb.RegisterSqlServer(s, &server{})
	reflection.Register(s)
	if err := s.Serve(lis); err != nil {
		log.Fatal().Err(err).Msg("failed to serve")
	}
}
