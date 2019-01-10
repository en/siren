package main

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/dgrijalva/jwt-go"
	"github.com/gofrs/uuid"
	"github.com/golang/protobuf/ptypes"
	timestamp "github.com/golang/protobuf/ptypes/timestamp"
	"github.com/gorilla/mux"

	pb "github.com/en/siren/protos"
	log "github.com/en/siren/utils/glog"
)

func currenciesHandler(w http.ResponseWriter, req *http.Request) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	r, err := matchClient.GetCurrencies(ctx, &pb.EmptyRequest{})
	if err != nil {
		// TODO: return error message
		log.Error().Err(err).Msg("failed to get currencies")
		return
	}
	writeInnerSlice(w, r)
}

func symbolsHandler(w http.ResponseWriter, req *http.Request) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	r, err := matchClient.GetSymbols(ctx, &pb.EmptyRequest{})
	if err != nil {
		// TODO: return error message
		log.Error().Err(err).Msg("failed to get currency pairs")
		return
	}
	writeInnerSlice(w, r)
}

func balancesHandler(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	token, _ := req.Context().Value("user").(*jwt.Token)
	claims, _ := token.Claims.(jwt.MapClaims)
	sub, _ := claims["sub"].(string)

	request := new(pb.BalancesRequest)
	request.UserId = sub
	request.Currency = vars["currency"]

	log.Info().Str("user_id", sub).Str("currency", vars["currency"]).Msg("query balance")

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	r, err := matchClient.Balances(ctx, request)
	if err != nil {
		// TODO: return error message
		log.Error().Err(err).Msg("failed to get balances")
		return
	}

	if vars["currency"] != "" {
		if len(r.Balances) != 1 {
			return
		}
		writeProto(w, r.Balances[0])
	} else {
		if r.Balances == nil {
			r.Balances = make([]*pb.Balance, 0)
		}
		writeInnerSlice(w, r)
	}
}

func transfersHandler(w http.ResponseWriter, req *http.Request) {
	token, _ := req.Context().Value("user").(*jwt.Token)
	claims, _ := token.Claims.(jwt.MapClaims)
	sub, _ := claims["sub"].(string)

	var (
		since *timestamp.Timestamp
		until *timestamp.Timestamp
		limit int64
		err   error
	)
	if req.FormValue("since") != "" {
		t, err := time.Parse(time.RFC3339Nano, req.FormValue("since"))
		if err != nil {
			log.Error().Err(err).Msg("parsing since failed")
			return
		}
		since, err = ptypes.TimestampProto(t)
		if err != nil {
			log.Error().Err(err).Msg("parsing since failed")
			return
		}
	}
	if req.FormValue("until") != "" {
		t, err := time.Parse(time.RFC3339Nano, req.FormValue("until"))
		if err != nil {
			log.Error().Err(err).Msg("parsing until failed")
			return
		}
		until, err = ptypes.TimestampProto(t)
		if err != nil {
			log.Error().Err(err).Msg("parsing until failed")
			return
		}
	}
	if req.FormValue("limit") != "" {
		limit, err = strconv.ParseInt(req.FormValue("limit"), 10, 32)
		if err != nil {
			log.Error().Err(err).Msg("strconv failed")
			return
		}
	}
	if limit == 0 {
		limit = 10
	}
	if limit > 50 {
		limit = 50
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	r, err := sqlClient.Transfers(ctx, &pb.TransfersRequest{
		UserId:   sub,
		Currency: req.FormValue("currency"),
		Type:     req.FormValue("type"),
		Since:    since,
		Until:    until,
		Limit:    int32(limit),
	})
	if err != nil {
		// TODO: return error message
		log.Error().Err(err).Msg("failed to get transfers")
		return
	}
	writeInnerSlice(w, r)
}

func depositsAddressHandler(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	token, _ := req.Context().Value("user").(*jwt.Token)
	claims, _ := token.Claims.(jwt.MapClaims)
	sub, _ := claims["sub"].(string)

	log.Info().Str("user", sub).Msg("query address")

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	r, err := walletClient.GetAddress(ctx, &pb.GetAddressRequest{UserId: sub, Currency: vars["currency"]})
	if err != nil {
		// TODO: return error message
		log.Error().Err(err).Msg("get address")
		return
	}
	writeProto(w, r)
}

func withdrawalsHandler(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	token, _ := req.Context().Value("user").(*jwt.Token)
	claims, _ := token.Claims.(jwt.MapClaims)
	sub, _ := claims["sub"].(string)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	r, err := walletClient.Withdraw(ctx, &pb.WithdrawRequest{UserId: sub, Currency: vars["currency"], Amount: req.PostFormValue("amount"), Address: req.PostFormValue("address")})
	if err != nil {
		// TODO: return error message
		log.Error().Err(err).Msg("withdraw")
		return
	}
	writeProto(w, r)
}

func newOrderHandler(w http.ResponseWriter, req *http.Request) {
	token, _ := req.Context().Value("user").(*jwt.Token)
	claims, _ := token.Claims.(jwt.MapClaims)
	sub, _ := claims["sub"].(string)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	r, err := matchClient.NewOrder(ctx, &pb.NewOrderRequest{
		UserId:        sub,
		ClientOrderId: req.PostFormValue("client_order_id"),
		Symbol:        req.PostFormValue("symbol"),
		Type:          req.PostFormValue("type"),
		Side:          req.PostFormValue("side"),
		Price:         req.PostFormValue("price"),
		Amount:        req.PostFormValue("amount"),
	})
	if err != nil {
		// TODO: return error message
		log.Error().Err(err).Msg("get new order")
		return
	}
	writeProto(w, r.Order)
}

func ordersHandler(w http.ResponseWriter, req *http.Request) {
	token, _ := req.Context().Value("user").(*jwt.Token)
	claims, _ := token.Claims.(jwt.MapClaims)
	sub, _ := claims["sub"].(string)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	r, err := matchClient.Orders(ctx, &pb.OrdersRequest{
		UserId: sub,
		Symbol: req.FormValue("symbol"),
	})
	if err != nil {
		// TODO: return error message
		log.Error().Err(err).Msg("get orders failed")
		return
	}
	if r.Orders == nil {
		r.Orders = make([]*pb.Order, 0)
	}
	writeInnerSlice(w, r)
}

func mgmtBalanceHandler(w http.ResponseWriter, req *http.Request) {
	id, err := uuid.NewV4()
	if err != nil {
		log.Printf("failed to generate UUID: %v", err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	r, err := matchClient.UpdateBalance(ctx, &pb.UpdateBalanceRequest{
		Id:       id.String(),
		Type:     req.PostFormValue("type"),
		UserId:   req.PostFormValue("user_id"),
		Currency: req.PostFormValue("currency"),
		Amount:   req.PostFormValue("amount"),
	})

	// TODO: log to db

	writeProto(w, r)
}
