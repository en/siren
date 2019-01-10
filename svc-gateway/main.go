package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/auth0/go-jwt-middleware"
	"github.com/dgrijalva/jwt-go"
	"github.com/golang/protobuf/jsonpb"
	"github.com/gorilla/mux"
	"github.com/rs/cors"
	"github.com/spf13/viper"
	"github.com/urfave/negroni"
	"google.golang.org/grpc"

	pb "github.com/en/siren/protos"
	log "github.com/en/siren/utils/glog"
)

var walletClient pb.WalletClient
var sqlClient pb.SqlClient
var memorystoreClient pb.MemorystoreClient
var matchClient pb.MatchClient

type Jwks struct {
	Keys []JSONWebKeys `json:"keys"`
}

type JSONWebKeys struct {
	Kty string   `json:"kty"`
	Kid string   `json:"kid"`
	Use string   `json:"use"`
	N   string   `json:"n"`
	E   string   `json:"e"`
	X5c []string `json:"x5c"`
}

func main() {
	log.Info().Msg("Starting siren-gateway")

	viper.SetEnvPrefix("gateway")
	viper.AutomaticEnv()
	replacer := strings.NewReplacer("-", "_")
	viper.SetEnvKeyReplacer(replacer)

	jwtMiddleware := jwtmiddleware.New(jwtmiddleware.Options{
		ValidationKeyGetter: func(token *jwt.Token) (interface{}, error) {
			// Verify 'aud' claim
			aud := viper.GetString("auth0-audience")
			checkAud := token.Claims.(jwt.MapClaims).VerifyAudience(aud, false)
			if !checkAud {
				return token, errors.New("Invalid audience.")
			}
			// Verify 'iss' claim
			iss := "https://" + viper.GetString("auth0-domain") + "/"
			checkIss := token.Claims.(jwt.MapClaims).VerifyIssuer(iss, false)
			if !checkIss {
				return token, errors.New("Invalid issuer.")
			}

			cert, err := getPemCert(token)
			if err != nil {
				panic(err.Error())
			}

			result, _ := jwt.ParseRSAPublicKeyFromPEM([]byte(cert))
			return result, nil
		},
		SigningMethod: jwt.SigningMethodRS256,
	})

	{
		conn, err := grpc.Dial("siren-match:50051", grpc.WithInsecure())
		if err != nil {
			log.Fatal().Err(err).Msg("did not connect")
		}
		defer conn.Close()
		matchClient = pb.NewMatchClient(conn)
		log.Info().Msg("connected to siren-match:50051")
	}
	{
		conn, err := grpc.Dial("siren-sql:50051", grpc.WithInsecure())
		if err != nil {
			log.Fatal().Err(err).Msg("did not connect")
		}
		defer conn.Close()
		sqlClient = pb.NewSqlClient(conn)
		log.Info().Msg("connected to siren-sql:50051")
	}
	{
		conn, err := grpc.Dial("siren-memorystore:50051", grpc.WithInsecure())
		if err != nil {
			log.Fatal().Err(err).Msg("did not connect")
		}
		defer conn.Close()
		memorystoreClient = pb.NewMemorystoreClient(conn)
		log.Info().Msg("connected to siren-memorystore:50051")
	}
	{
		conn, err := grpc.Dial("siren-wallet:50051", grpc.WithInsecure())
		if err != nil {
			log.Fatal().Err(err).Msg("did not connect")
		}
		defer conn.Close()
		walletClient = pb.NewWalletClient(conn)
		log.Info().Msg("connected to siren-wallet:50051")
	}

	r := mux.NewRouter()
	r.HandleFunc("/healthz", func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		io.WriteString(w, `{}`)
	}).Methods("GET")

	// /v1
	v1 := mux.NewRouter().PathPrefix("/v1").Subrouter()
	v1.HandleFunc("/currencies", currenciesHandler).Methods("GET")
	v1.HandleFunc("/symbols", symbolsHandler).Methods("GET")

	v1.Handle("/balances", negroni.New(
		negroni.HandlerFunc(jwtMiddleware.HandlerWithNext),
		negroni.Wrap(http.HandlerFunc(balancesHandler)))).Methods("GET")
	v1.Handle("/balances/{currency:[A-Z]+}", negroni.New(
		negroni.HandlerFunc(jwtMiddleware.HandlerWithNext),
		negroni.Wrap(http.HandlerFunc(balancesHandler)))).Methods("GET")
	v1.Handle("/transfers", negroni.New(
		negroni.HandlerFunc(jwtMiddleware.HandlerWithNext),
		negroni.Wrap(http.HandlerFunc(transfersHandler)))).Methods("GET")
	v1.Handle("/deposits/{currency:[A-Z]+}/address", negroni.New(
		negroni.HandlerFunc(jwtMiddleware.HandlerWithNext),
		negroni.Wrap(http.HandlerFunc(depositsAddressHandler)))).Methods("GET")
	v1.Handle("/withdrawals/{currency:[A-Z]+}", negroni.New(
		negroni.HandlerFunc(jwtMiddleware.HandlerWithNext),
		negroni.Wrap(http.HandlerFunc(withdrawalsHandler)))).Methods("POST")
	v1.Handle("/orders", negroni.New(
		negroni.HandlerFunc(jwtMiddleware.HandlerWithNext),
		negroni.Wrap(http.HandlerFunc(newOrderHandler)))).Methods("POST")
	v1.Handle("/orders", negroni.New(
		negroni.HandlerFunc(jwtMiddleware.HandlerWithNext),
		negroni.Wrap(http.HandlerFunc(ordersHandler)))).Methods("GET")

	v1.HandleFunc("/mgmt/balance", mgmtBalanceHandler).Methods("POST")

	v1.HandleFunc("/asset.summary", func(w http.ResponseWriter, req *http.Request) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		request := new(pb.GetAssetSummaryRequest)
		if values, ok := req.URL.Query()["currency"]; ok {
			for _, value := range values {
				request.Currencies = append(request.Currencies, value)
			}
		}

		r, err := matchClient.GetAssetSummary(ctx, request)
		if err != nil {
			// TODO: return error message
			log.Error().Err(err).Msg("failed to get asset summary")
			return
		}

		m := jsonpb.Marshaler{EmitDefaults: true}
		err = m.Marshal(w, r)
		if err != nil {
			log.Error().Err(err).Msg("marshal get asset summary reply")
		}
	}).Methods("GET")

	v1.Handle("/order.cancel/{order_id}", negroni.New(
		negroni.HandlerFunc(jwtMiddleware.HandlerWithNext),
		negroni.Wrap(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			token, _ := req.Context().Value("user").(*jwt.Token)
			claims, _ := token.Claims.(jwt.MapClaims)
			sub, _ := claims["sub"].(string)

			vars := mux.Vars(req)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			r, err := matchClient.OrderCancel(ctx, &pb.OrderCancelRequest{
				UserId:  sub,
				Symbol:  req.FormValue("symbol"),
				OrderId: vars["order_id"],
			})
			if err != nil {
				// TODO: return error message
				log.Error().Err(err).Msg("marshal order cancel")
				return
			}
			m := jsonpb.Marshaler{EmitDefaults: true}
			err = m.Marshal(w, r)
			if err != nil {
				log.Error().Err(err).Msg("marshal order cancel reply")
			}
		})))).Methods("DELETE")

	v1.HandleFunc("/order.book/{symbol}", func(w http.ResponseWriter, req *http.Request) {
		vars := mux.Vars(req)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		side, err := strconv.ParseInt(req.FormValue("side"), 10, 32)
		if err != nil {
			log.Error().Err(err).Msg("strconv failed")
			return
		}
		limit, err := strconv.ParseInt(req.FormValue("limit"), 10, 64)
		if err != nil {
			log.Error().Err(err).Msg("strconv failed")
			return
		}

		r, err := matchClient.OrderBook(ctx, &pb.OrderBookRequest{
			Symbol: vars["symbol"],
			Side:   int32(side),
			Limit:  uint64(limit),
		})
		if err != nil {
			// TODO: return error message
			log.Error().Err(err).Msg("marshal order book")
			return
		}
		m := jsonpb.Marshaler{EmitDefaults: true}
		err = m.Marshal(w, r)
		if err != nil {
			log.Error().Err(err).Msg("marshal order book reply")
		}
	}).Methods("GET")

	v1.HandleFunc("/order.depth/{symbol}", func(w http.ResponseWriter, req *http.Request) {
		vars := mux.Vars(req)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		limit, err := strconv.ParseInt(req.FormValue("limit"), 10, 64)
		if err != nil {
			log.Error().Err(err).Msg("strconv failed")
			return
		}

		r, err := matchClient.OrderBookDepth(ctx, &pb.OrderBookDepthRequest{
			Symbol: vars["symbol"],
			Limit:  uint64(limit),
		})
		if err != nil {
			// TODO: return error message
			log.Error().Err(err).Msg("marshal order depth")
			return
		}
		m := jsonpb.Marshaler{EmitDefaults: true}
		err = m.Marshal(w, r)
		if err != nil {
			log.Error().Err(err).Msg("marshal order depth reply")
		}
	}).Methods("GET")

	v1.HandleFunc("/order.pending_detail/{symbol}/{order_id}", func(w http.ResponseWriter, req *http.Request) {
		vars := mux.Vars(req)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		r, err := matchClient.OrderDetail(ctx, &pb.OrderDetailRequest{
			Symbol:  vars["symbol"],
			OrderId: vars["order_id"],
		})
		if err != nil {
			// TODO: return error message
			log.Error().Err(err).Msg("marshal order pending_detail")
			return
		}
		m := jsonpb.Marshaler{EmitDefaults: true}
		err = m.Marshal(w, r)
		if err != nil {
			log.Error().Err(err).Msg("marshal order pending_detail reply")
		}
	}).Methods("GET")

	v1.HandleFunc("/order.deals/{order_id}", func(w http.ResponseWriter, req *http.Request) {
		vars := mux.Vars(req)

		// TODO: use req.Context() ?
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		offset, err := strconv.ParseInt(req.FormValue("offset"), 10, 32)
		if err != nil {
			log.Error().Err(err).Msg("strconv failed")
			return
		}
		limit, err := strconv.ParseInt(req.FormValue("limit"), 10, 32)
		if err != nil {
			log.Error().Err(err).Msg("strconv failed")
			return
		}
		r, err := sqlClient.GetOrderDeals(ctx, &pb.GetOrderDealsRequest{
			OrderId: vars["order_id"],
			Offset:  int32(offset),
			Limit:   int32(limit),
		})
		if err != nil {
			// TODO: return error message
			log.Error().Err(err).Msg("marshal get order deals")
			return
		}
		b, err := json.Marshal(r.Items)
		if err != nil {
			log.Error().Err(err).Msg("marshal get order deals reply")
			return
		}

		w.Write(b)
	}).Methods("GET")

	v1.Handle("/order.finished", negroni.New(
		negroni.HandlerFunc(jwtMiddleware.HandlerWithNext),
		negroni.Wrap(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			token, _ := req.Context().Value("user").(*jwt.Token)
			claims, _ := token.Claims.(jwt.MapClaims)
			sub, _ := claims["sub"].(string)

			// TODO: use req.Context() ?
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			startTime, err := strconv.ParseInt(req.FormValue("start_time"), 10, 64)
			if err != nil {
				log.Error().Err(err).Msg("strconv failed")
				return
			}
			endTime, err := strconv.ParseInt(req.FormValue("end_time"), 10, 64)
			if err != nil {
				log.Error().Err(err).Msg("strconv failed")
				return
			}
			offset, err := strconv.ParseInt(req.FormValue("offset"), 10, 32)
			if err != nil {
				log.Error().Err(err).Msg("strconv failed")
				return
			}
			limit, err := strconv.ParseInt(req.FormValue("limit"), 10, 32)
			if err != nil {
				log.Error().Err(err).Msg("strconv failed")
				return
			}
			side, err := strconv.ParseInt(req.FormValue("side"), 10, 32)
			if err != nil {
				log.Error().Err(err).Msg("strconv failed")
				return
			}
			r, err := sqlClient.GetOrderHistory(ctx, &pb.GetOrderHistoryRequest{
				UserId:    sub,
				Market:    req.FormValue("symbol"),
				StartTime: startTime,
				EndTime:   endTime,
				Offset:    int32(offset),
				Limit:     int32(limit),
				Side:      int32(side),
			})
			if err != nil {
				// TODO: return error message
				log.Error().Err(err).Msg("marshal get order finished")
				return
			}
			b, err := json.Marshal(r.Items)
			if err != nil {
				log.Error().Err(err).Msg("marshal get order finished reply")
				return
			}

			w.Write(b)
		})))).Methods("GET")

	v1.HandleFunc("/order.finished_detail/{order_id}", func(w http.ResponseWriter, req *http.Request) {
		vars := mux.Vars(req)

		// TODO: use req.Context() ?
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		r, err := sqlClient.GetOrderDetailFinished(ctx, &pb.GetOrderDetailFinishedRequest{
			OrderId: vars["order_id"],
		})
		if err != nil {
			// TODO: return error message
			log.Error().Err(err).Msg("marshal get order finished detail")
			return
		}
		b, err := json.Marshal(r)
		if err != nil {
			log.Error().Err(err).Msg("marshal get order finished detail reply")
			return
		}

		w.Write(b)
	}).Methods("GET")

	v1.HandleFunc("/market.last/{symbol}", func(w http.ResponseWriter, req *http.Request) {
		vars := mux.Vars(req)

		// TODO: use req.Context() ?
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		r, err := memorystoreClient.MarketLast(ctx, &pb.MarketLastRequest{
			Market: vars["symbol"],
		})
		if err != nil {
			// TODO: return error message
			log.Error().Err(err).Msg("marshal market last")
			return
		}
		m := jsonpb.Marshaler{EmitDefaults: true}
		err = m.Marshal(w, r)
		if err != nil {
			log.Error().Err(err).Msg("marshal market last reply")
		}
	}).Methods("GET")

	v1.HandleFunc("/market.deals/{symbol}", func(w http.ResponseWriter, req *http.Request) {
		vars := mux.Vars(req)

		// TODO: use req.Context() ?
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		limit, err := strconv.ParseInt(req.FormValue("limit"), 10, 32)
		if err != nil {
			log.Error().Err(err).Msg("strconv failed")
			return
		}
		r, err := memorystoreClient.MarketDeals(ctx, &pb.MarketDealsRequest{
			Market: vars["symbol"],
			Limit:  int32(limit),
			LastId: 0,
		})
		if err != nil {
			// TODO: return error message
			log.Error().Err(err).Msg("marshal market deals")
			return
		}
		m := jsonpb.Marshaler{EmitDefaults: true}
		err = m.Marshal(w, r)
		if err != nil {
			log.Error().Err(err).Msg("marshal market deals reply")
		}
	}).Methods("GET")

	v1.HandleFunc("/market.kline/{symbol}", func(w http.ResponseWriter, req *http.Request) {
		vars := mux.Vars(req)

		// TODO: use req.Context() ?
		ctx := context.Background()
		// ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		// defer cancel()
		start, err := strconv.ParseInt(req.FormValue("start"), 10, 64)
		if err != nil {
			log.Error().Err(err).Msg("strconv failed")
			return
		}
		end, err := strconv.ParseInt(req.FormValue("end"), 10, 64)
		if err != nil {
			log.Error().Err(err).Msg("strconv failed")
			return
		}
		interval, err := strconv.ParseInt(req.FormValue("interval"), 10, 64)
		if err != nil {
			log.Error().Err(err).Msg("strconv failed")
			return
		}
		r, err := memorystoreClient.MarketKline(ctx, &pb.MarketKlineRequest{
			Market:   vars["symbol"],
			Start:    start,
			End:      end,
			Interval: interval,
		})
		if err != nil {
			// TODO: return error message
			log.Error().Err(err).Msg("marshal market kline")
			return
		}
		m := jsonpb.Marshaler{EmitDefaults: true}
		err = m.Marshal(w, r)
		if err != nil {
			log.Error().Err(err).Msg("marshal market kline reply")
		}
	}).Methods("GET")

	v1.HandleFunc("/market.status/{symbol}", func(w http.ResponseWriter, req *http.Request) {
		vars := mux.Vars(req)

		// TODO: use req.Context() ?
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		period, err := strconv.ParseInt(req.FormValue("period"), 10, 32)
		if err != nil {
			log.Error().Err(err).Msg("strconv failed")
			return
		}
		r, err := memorystoreClient.MarketStatus(ctx, &pb.MarketStatusRequest{
			Market: vars["symbol"],
			Period: int32(period),
		})
		if err != nil {
			// TODO: return error message
			log.Error().Err(err).Msg("marshal market status")
			return
		}
		m := jsonpb.Marshaler{EmitDefaults: true}
		err = m.Marshal(w, r)
		if err != nil {
			log.Error().Err(err).Msg("marshal market status reply")
		}
	}).Methods("GET")

	v1.HandleFunc("/market.status_today/{symbol}", func(w http.ResponseWriter, req *http.Request) {
		vars := mux.Vars(req)

		// TODO: use req.Context() ?
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		r, err := memorystoreClient.MarketStatusToday(ctx, &pb.MarketStatusTodayRequest{
			Market: vars["symbol"],
		})
		if err != nil {
			// TODO: return error message
			log.Error().Err(err).Msg("marshal market status today")
			return
		}
		m := jsonpb.Marshaler{EmitDefaults: true}
		err = m.Marshal(w, r)
		if err != nil {
			log.Error().Err(err).Msg("marshal market status today")
		}
	}).Methods("GET")

	v1.Handle("/market.user_deals", negroni.New(
		negroni.HandlerFunc(jwtMiddleware.HandlerWithNext),
		negroni.Wrap(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			token, _ := req.Context().Value("user").(*jwt.Token)
			claims, _ := token.Claims.(jwt.MapClaims)
			sub, _ := claims["sub"].(string)

			// TODO: use req.Context() ?
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			offset, err := strconv.ParseInt(req.FormValue("offset"), 10, 32)
			if err != nil {
				log.Error().Err(err).Msg("strconv failed")
				return
			}
			limit, err := strconv.ParseInt(req.FormValue("limit"), 10, 32)
			if err != nil {
				log.Error().Err(err).Msg("strconv failed")
				return
			}
			r, err := sqlClient.GetMarketUserDeals(ctx, &pb.GetMarketUserDealsRequest{
				UserId: sub,
				Market: req.FormValue("symbol"),
				Offset: int32(offset),
				Limit:  int32(limit),
			})
			if err != nil {
				// TODO: return error message
				log.Error().Err(err).Msg("marshal get market user deals")
				return
			}
			b, err := json.Marshal(r.Items)
			if err != nil {
				log.Error().Err(err).Msg("marshal get market user deals reply")
				return
			}

			w.Write(b)
		})))).Methods("GET")

	v1.HandleFunc("/market.summary", func(w http.ResponseWriter, req *http.Request) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		request := new(pb.GetMarketSummaryRequest)
		if values, ok := req.URL.Query()["symbol"]; ok {
			for _, value := range values {
				request.Symbols = append(request.Symbols, value)
			}
		}

		r, err := matchClient.GetMarketSummary(ctx, request)
		if err != nil {
			// TODO: return error message
			log.Error().Err(err).Msg("failed to get market summary")
			return
		}

		m := jsonpb.Marshaler{EmitDefaults: true}
		err = m.Marshal(w, r)
		if err != nil {
			log.Error().Err(err).Msg("marshal get market summary reply")
		}
	}).Methods("GET")

	l := negroni.NewLogger()
	l.SetFormat("{{.Status}} | \t {{.Duration}} | {{.Hostname}} | {{.Method}} {{.Path}} \n")
	c := cors.New(cors.Options{
		AllowedOrigins:   []string{"*"},
		AllowedMethods:   []string{"GET", "POST", "DELETE"},
		AllowCredentials: true,
		AllowedHeaders:   []string{"Authorization"},
	})
	r.PathPrefix("/v1").Handler(negroni.New(
		l,
		c,
		negroni.Wrap(v1),
	))

	n := negroni.New()
	n.Use(negroni.NewRecovery())
	n.UseHandler(r)

	n.Run(":3000")
}

func getPemCert(token *jwt.Token) (string, error) {
	cert := ""
	resp, err := http.Get("https://" + viper.GetString("auth0-domain") + "/.well-known/jwks.json")

	if err != nil {
		return cert, err
	}
	defer resp.Body.Close()

	var jwks = Jwks{}
	err = json.NewDecoder(resp.Body).Decode(&jwks)

	if err != nil {
		return cert, err
	}

	for k, _ := range jwks.Keys {
		if token.Header["kid"] == jwks.Keys[k].Kid {
			cert = "-----BEGIN CERTIFICATE-----\n" + jwks.Keys[k].X5c[0] + "\n-----END CERTIFICATE-----"
		}
	}

	if cert == "" {
		err := errors.New("Unable to find appropriate key.")
		return cert, err
	}

	return cert, nil
}
