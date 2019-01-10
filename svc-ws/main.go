package main

import (
	"container/list"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/dgrijalva/jwt-go"
	"github.com/gorilla/mux"
	"github.com/gorilla/websocket"
	"github.com/spf13/viper"
	"github.com/urfave/negroni"
	"google.golang.org/grpc"

	pb "github.com/en/siren/protos"
	log "github.com/en/siren/utils/glog"
)

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

func validationKeyGetter(token *jwt.Token) (interface{}, error) {
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

type userOrder struct {
	Id        int    `json:"id"`
	Ctime     int64  `json:"ctime"`
	TakerFee  string `json:"taker_fee"`
	Market    string `json:"market"`
	Side      int    `json:"side"`
	Source    string `json:"source"`
	DealMoney string `json:"deal_money"`
	Type      int    `json:"type"`
	User      string `json:"user"`
	Mtime     int64  `json:"mtime"`
	DealStock string `json:"deal_stock"`
	Price     string `json:"price"`
	Amount    string `json:"amount"`
	MakerFee  string `json:"maker_fee"`
	Left      string `json:"left"`
	DealFee   string `json:"deal_fee"`
}
type orderMessage struct {
	Event int64           `json:"event"`
	Order json.RawMessage `json:"order"`
	Stock string          `json:"stock"`
	Money string          `json:"money"`
}

type rpcReply struct {
	Result json.RawMessage `json:"result"`
	Id     json.RawMessage `json:"id"`
	Error  json.RawMessage `json:"error"`
}

type rpcToClient struct {
	Method string          `json:"method"`
	Params json.RawMessage `json:"result"`
	Id     json.RawMessage `json:"id"`
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

type dealsVars struct {
	Conns       *list.List
	LastTradeId int64
}

type orderVars struct {
	Conn      *websocket.Conn
	ProductId string
}

type assetVars struct {
	Conn *websocket.Conn
	Name string
}

var kline map[string]*list.List
var depth map[string]*list.List
var today map[string]*list.List
var deals map[string]dealsVars
var order map[string]*list.List
var asset map[string]*list.List

func metimer(interval time.Duration) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			for k, v := range depth {
				if v.Len() == 0 {
					break
				}
				fields := strings.Split(k, "-")
				// productId := strings.Join(fields[:len(fields)-1], "-")
				productId := strings.Join(fields, "")
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				r, err := matchClient.OrderBookDepth(ctx, &pb.OrderBookDepthRequest{
					Symbol: productId,
					Limit:  100,
				})
				if err != nil {
					// TODO: return error message
					log.Error().Msg("failed to get order book depth")
					continue
				}
				var rtc Message
				rtc.Type = "depth-snapshot"
				rtc.ProductId = k
				// TODO: len(r.Asks) == 0
				for _, data := range r.Asks {
					rtc.Asks = append(rtc.Asks, SnapshotEntry{Price: fmt.Sprintf("%.6f", data.Price), Size: fmt.Sprintf("%.6f", data.Quantity)})
				}
				for _, data := range r.Bids {
					rtc.Bids = append(rtc.Bids, SnapshotEntry{Price: fmt.Sprintf("%.6f", data.Price), Size: fmt.Sprintf("%.6f", data.Quantity)})
				}
				b, err := json.Marshal(rtc)
				if err != nil {
					log.Error().Msg("marshal reply")
					continue
				}
				// TODO: compare last broadcast time
				for e := v.Front(); e != nil; {
					next := e.Next()
					conn, ok := e.Value.(*websocket.Conn)
					if ok {
						err := conn.WriteMessage(websocket.TextMessage, b)
						if err != nil {
							log.Warn().Msg("failed to write message")
							v.Remove(e)
						}
					} else {
						log.Error().Msg("type assertion failed")
						v.Remove(e)
					}
					e = next
				}
			}
		}
	}
}

func mptimer(interval time.Duration) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	ctx := context.Background()
	for {
		select {
		case <-ticker.C:
			for k, v := range today {
				if v.Len() == 0 {
					break
				}
				fields := strings.Split(k, "-")
				// productId := strings.Join(fields[:len(fields)-1], "-")
				productId := strings.Join(fields, "")
				r, err := memorystoreClient.MarketStatusToday(ctx, &pb.MarketStatusTodayRequest{
					Market: productId,
				})
				if err != nil {
					// TODO: return error message
					log.Error().Msg("get market status today")
					continue
				}
				var rtc Message
				rtc.Type = "ticker"
				rtc.ProductId = k
				rtc.BestAsk = "0"
				rtc.BestBid = "0"
				rtc.High24h = fmt.Sprintf("%.6f", r.High)
				rtc.Low24h = fmt.Sprintf("%.6f", r.Low)
				rtc.Open24h = fmt.Sprintf("%.6f", r.Open)
				rtc.Price = fmt.Sprintf("%.6f", r.Last)
				rtc.Volume24h = fmt.Sprintf("%.6f", r.Volume)
				rtc.Volume30d = fmt.Sprintf("%.6f", r.Deal) // TODO: for debugging
				b, err := json.Marshal(rtc)
				if err != nil {
					log.Error().Msg("marshal reply")
					continue
				}
				for e := v.Front(); e != nil; {
					next := e.Next()
					conn, ok := e.Value.(*websocket.Conn)
					if ok {
						err := conn.WriteMessage(websocket.TextMessage, b)
						if err != nil {
							log.Warn().Msg("failed to write message")
							v.Remove(e)
						}
					} else {
						log.Error().Msg("type assertion failed")
						v.Remove(e)
					}
					e = next
				}
			}

			for k, v := range deals {
				if v.Conns.Len() == 0 {
					break
				}
				fields := strings.Split(k, "-")
				// productId := strings.Join(fields[:len(fields)-1], "-")
				productId := strings.Join(fields, "")
				r, err := memorystoreClient.MarketDeals(ctx, &pb.MarketDealsRequest{
					Market: productId,
					Limit:  100,
					LastId: uint64(v.LastTradeId),
				})
				if err != nil {
					// TODO: return error message
					log.Error().Msg("get market deals")
					continue
				}

				for _, item := range r.Items {
					var rtc Message
					rtc.Type = "match"
					rtc.ProductId = k
					rtc.TradeId = int(item.Id)
					rtc.Side = item.Type
					rtc.Size = item.Amount
					rtc.Price = item.Price
					rtc.Time = Time(time.Unix(0, int64(item.Time*1000*1000*1000)))
					b, err := json.Marshal(rtc)
					if err != nil {
						log.Error().Msg("marshal reply")
						continue
					}
					for e := v.Conns.Front(); e != nil; {
						next := e.Next()
						conn, ok := e.Value.(*websocket.Conn)
						if ok {
							err := conn.WriteMessage(websocket.TextMessage, b)
							if err != nil {
								log.Warn().Msg("failed to write message")
								v.Conns.Remove(e)
							}
						} else {
							log.Error().Msg("type assertion failed")
							v.Conns.Remove(e)
						}
						e = next
					}
				}
				if len(r.Items) > 0 {
					fmt.Println("trade items:", r.Items)
					deals[k] = dealsVars{deals[k].Conns, int64(r.Items[0].Id)}
				}
			}
		}
	}
}

func writeErrorMessage(conn *websocket.Conn, message string) {
	msg := Message{
		Type:    "error",
		Message: message,
	}
	b, err := json.Marshal(msg)
	if err != nil {
		log.Error().Str("msg", message).Msg("marshal error message")
		return
	}

	err = conn.WriteMessage(websocket.TextMessage, b)
	if err != nil {
		log.Warn().Msg("write error message")
	}
}

func writeAuthSuccessMessage(conn *websocket.Conn) {
	msg := Message{
		Type: "auth-success",
	}
	b, err := json.Marshal(msg)
	if err != nil {
		log.Error().Msg("marshal auth success message")
		return
	}

	err = conn.WriteMessage(websocket.TextMessage, b)
	if err != nil {
		log.Warn().Msg("write auth-success message")
	}
}

func writeSuccessMessage(conn *websocket.Conn) {
	msg := Message{
		Type:     "subscriptions",
		Channels: []MessageChannel{},
	}
	b, err := json.Marshal(msg)
	if err != nil {
		log.Error().Msg("marshal success message")
		return
	}

	err = conn.WriteMessage(websocket.TextMessage, b)
	if err != nil {
		log.Warn().Msg("write success message")
	}
}

func subscribeKline(conn *websocket.Conn, productIds []string) {
	for _, id := range productIds {
		_, ok := kline[id]
		if !ok {
			kline[id] = list.New()
		}
		exist := false
		for e := kline[id].Front(); e != nil; e = e.Next() {
			if e.Value == conn {
				exist = true
				break
			}
		}
		if !exist {
			kline[id].PushBack(conn)
		}
	}
	writeSuccessMessage(conn)
}

func subscribeDepth(conn *websocket.Conn, productIds []string) {
	for _, id := range productIds {
		_, ok := depth[id]
		if !ok {
			depth[id] = list.New()
		}
		exist := false
		for e := depth[id].Front(); e != nil; e = e.Next() {
			if e.Value == conn {
				exist = true
				break
			}
		}
		if !exist {
			depth[id].PushBack(conn)
		}
	}
	writeSuccessMessage(conn)
}

func subscribeTrade(conn *websocket.Conn, lastTradeId int, productIds []string) {
	for _, id := range productIds {
		_, ok := deals[id]
		if !ok {
			deals[id] = dealsVars{list.New(), int64(lastTradeId)}
		}
		exist := false
		for e := deals[id].Conns.Front(); e != nil; e = e.Next() {
			if e.Value == conn {
				exist = true
				break
			}
		}
		if !exist {
			deals[id].Conns.PushBack(conn)
			// TODO: logger
			deals[id] = dealsVars{deals[id].Conns, int64(lastTradeId)}
		}
	}
	writeSuccessMessage(conn)
}

func subscribeTicker(conn *websocket.Conn, productIds []string) {
	for _, id := range productIds {
		_, ok := today[id]
		if !ok {
			today[id] = list.New()
		}
		exist := false
		for e := today[id].Front(); e != nil; e = e.Next() {
			if e.Value == conn {
				exist = true
				break
			}
		}
		if !exist {
			today[id].PushBack(conn)
		}
	}
	writeSuccessMessage(conn)
}

func subscribeOrder(conn *websocket.Conn, subject string, productIds []string) {
	if subject == "" {
		log.Error().Msg("empty user token")
		return
	}
	_, ok := order[subject]
	if !ok {
		order[subject] = list.New()
	}
	for _, id := range productIds {
		exist := false
		for e := order[subject].Front(); e != nil; e = e.Next() {
			vars, ok := e.Value.(orderVars)
			if ok {
				if vars.Conn == conn && vars.ProductId == id {
					exist = true
					break
				}
			} else {
				log.Error().Msg("type assertion failed")
			}
		}
		if !exist {
			order[subject].PushBack(orderVars{conn, id})
		}
	}
	writeSuccessMessage(conn)
}

func subscribeAsset(conn *websocket.Conn, subject string, productIds []string) {
	if subject == "" {
		log.Error().Msg("empty user token")
		return
	}
	_, ok := asset[subject]
	if !ok {
		asset[subject] = list.New()
	}

	for _, id := range productIds {
		exist := false
		for e := asset[subject].Front(); e != nil; e = e.Next() {
			vars, ok := e.Value.(assetVars)
			if ok {
				if vars.Conn == conn && vars.Name == id {
					exist = true
					break
				}
			} else {
				log.Error().Msg("type assertion failed")
			}
		}
		if !exist {
			asset[subject].PushBack(assetVars{conn, id})
		}
	}
	writeSuccessMessage(conn)
}

func unsubscribeKline(conn *websocket.Conn, productIds []string) {
	for _, id := range productIds {
		_, ok := kline[id]
		if ok {
			for e := kline[id].Front(); e != nil; {
				next := e.Next()
				if e.Value == conn {
					kline[id].Remove(e)
					log.Info().Str("market", id).Msg("unsubscribed kline")
					break
				}
				e = next
			}
		}
	}
	writeSuccessMessage(conn)
}

func unsubscribeDepth(conn *websocket.Conn, productIds []string) {
	for _, id := range productIds {
		_, ok := depth[id]
		if ok {
			for e := depth[id].Front(); e != nil; {
				next := e.Next()
				if e.Value == conn {
					depth[id].Remove(e)
					log.Info().Str("market", id).Msg("unsubscribed depth")
					break
				}
				e = next
			}
		}
	}
	writeSuccessMessage(conn)
}

func unsubscribeTrade(conn *websocket.Conn, productIds []string) {
	for _, id := range productIds {
		_, ok := deals[id]
		if ok {
			for e := deals[id].Conns.Front(); e != nil; {
				next := e.Next()
				if e.Value == conn {
					deals[id].Conns.Remove(e)
					log.Info().Str("market", id).Msg("unsubscribed trade")
					break
				}
				e = next
			}
		}
	}
	writeSuccessMessage(conn)
}

func unsubscribeTicker(conn *websocket.Conn, productIds []string) {
	for _, id := range productIds {
		_, ok := today[id]
		if ok {
			for e := today[id].Front(); e != nil; {
				next := e.Next()
				if e.Value == conn {
					today[id].Remove(e)
					log.Info().Str("market", id).Msg("unsubscribed ticker")
					break
				}
				e = next
			}
		}
	}
	writeSuccessMessage(conn)
}

func unsubscribeOrder(conn *websocket.Conn, subject string, productIds []string) {
	if subject == "" {
		log.Error().Msg("empty user token")
		return
	}
	l, ok := order[subject]
	if !ok {
		log.Error().Str("user", subject).Msg("user haven't subscribed to any products")
		return
	}

	for _, id := range productIds {
		for e := l.Front(); e != nil; {
			next := e.Next()
			vars, ok := e.Value.(orderVars)
			if ok {
				if vars.Conn == conn && vars.ProductId == id {
					l.Remove(e)
					log.Info().Str("market", id).Msg("unsubscribed order")
					break
				}
			} else {
				log.Error().Msg("type assertion failed")
				l.Remove(e)
			}
			e = next
		}
	}
	writeSuccessMessage(conn)
}

func unsubscribeAsset(conn *websocket.Conn, subject string, productIds []string) {
	if subject == "" {
		log.Error().Msg("empty user token")
		return
	}
	l, ok := asset[subject]
	if !ok {
		log.Error().Str("user", subject).Msg("user haven't subscribed to any products")
		return
	}

	for _, id := range productIds {
		for e := l.Front(); e != nil; {
			next := e.Next()
			vars, ok := e.Value.(assetVars)
			if ok {
				if vars.Conn == conn && vars.Name == id {
					l.Remove(e)
					log.Info().Str("market", id).Msg("unsubscribed asset")
					break
				}
			} else {
				log.Error().Msg("type assertion failed")
				l.Remove(e)
			}
			e = next
		}
	}
	writeSuccessMessage(conn)
}

func home(w http.ResponseWriter, r *http.Request) {
	log.Info().Str("realIP", r.Header.Get("X-Forwarded-For")).Str("host", r.Host).Msg("Request Received.")
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Warn().Msg("Upgrade failed")
		return
	}
	defer conn.Close()
	var subject string

	for {
		_, message, err := conn.ReadMessage()
		if err != nil {
			log.Error().Msg("read from ws")
			break
		}

		log.Info().Str("msg", string(message)).Msg("receive from ws")

		var msg Message
		err = json.Unmarshal(message, &msg)
		if err != nil {
			log.Error().Msg("unmarshal message")
			writeErrorMessage(conn, "malformed JSON")
			break
		}

		switch msg.Type {
		case "auth":
			if msg.Token == "" {
				log.Error().Msg("empty user token")
				writeErrorMessage(conn, "empty user token")
				return
			}
			parsedToken, err := jwt.Parse(msg.Token, validationKeyGetter)
			if err != nil {
				log.Error().Msg("Error parsing token")
				writeErrorMessage(conn, "Error parsing token")
				return
			}
			if !parsedToken.Valid {
				log.Error().Interface("token", parsedToken).Msg("Token is invalid")
				writeErrorMessage(conn, "Token is invalid")
				return
			}
			claims, _ := parsedToken.Claims.(jwt.MapClaims)
			subject, _ = claims["sub"].(string)
			log.Info().Str("user", subject).Msg("auth")
			writeAuthSuccessMessage(conn)
		case "unauth":
			if msg.Token == "" {
				log.Error().Msg("empty user token")
				writeErrorMessage(conn, "empty user token")
				return
			}
			parsedToken, err := jwt.Parse(msg.Token, validationKeyGetter)
			if err != nil {
				log.Error().Msg("Error parsing token")
				writeErrorMessage(conn, "Error parsing token")
				return
			}
			if !parsedToken.Valid {
				log.Error().Interface("token", parsedToken).Msg("Token is invalid")
				writeErrorMessage(conn, "Token is invalid")
				return
			}
			claims, _ := parsedToken.Claims.(jwt.MapClaims)
			sub, _ := claims["sub"].(string)

			if subject == sub {
				log.Info().Str("user", subject).Msg("unauth")
				subject = ""
				writeAuthSuccessMessage(conn)
			} else {
				log.Info().Str("oldUser", subject).Str("newUser", sub).Msg("unauth failed, token not match")
				writeErrorMessage(conn, "token not match")
				return
			}
		case "subscribe":
			for _, c := range msg.Channels {
				switch c.Name {
				case "kline":
					subscribeKline(conn, c.ProductIds)
				case "depth":
					subscribeDepth(conn, c.ProductIds)
				case "ticker":
					subscribeTicker(conn, c.ProductIds)
				case "trade":
					subscribeTrade(conn, msg.LastTradeId, c.ProductIds)
				case "order":
					subscribeOrder(conn, subject, c.ProductIds)
				case "asset":
					subscribeAsset(conn, subject, c.ProductIds)
				default:
					log.Error().Str("name", c.Name).Msg("unknown channel")
					writeErrorMessage(conn, "unknown channel")
					return
				}
			}
		case "unsubscribe":
			for _, c := range msg.Channels {
				switch c.Name {
				case "kline":
					unsubscribeKline(conn, c.ProductIds)
				case "depth":
					unsubscribeDepth(conn, c.ProductIds)
				case "ticker":
					unsubscribeTicker(conn, c.ProductIds)
				case "trade":
					unsubscribeTrade(conn, c.ProductIds)
				case "order":
					unsubscribeOrder(conn, subject, c.ProductIds)
				case "asset":
					unsubscribeAsset(conn, subject, c.ProductIds)
				default:
					log.Error().Str("name", c.Name).Msg("unknown channel")
					writeErrorMessage(conn, "unknown channel")
					return
				}
			}
		default:
			log.Error().Str("type", msg.Type).Msg("unknown message type")
			writeErrorMessage(conn, "unknown message type")
			return
		}
	}
}

func handleMessages() {
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
		"group.id":                        "siren-ws",
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

	err = consumer.SubscribeTopics([]string{"orders", "balances"}, nil)

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
				case "balances":
					log.Info().Str("key", string(e.Key)).Str("value", string(e.Value)).Msg("receive balance message")
					var fields []json.RawMessage

					// var msg orderMessage
					err = json.Unmarshal(e.Value, &fields)
					if err != nil {
						log.Error().Msg("unmarshal balance message")
						continue
					}

					var sub string
					err = json.Unmarshal(fields[1], &sub)
					if err != nil {
						log.Error().Msg("unmarshal auth subject")
						continue
					}

					var name string
					err = json.Unmarshal(fields[2], &name)
					if err != nil {
						log.Error().Msg("unmarshal asset name")
						continue
					}

					l, ok := asset[sub]
					if ok {
						for e := l.Front(); e != nil; {
							next := e.Next()
							vars, ok := e.Value.(assetVars)
							if !ok {
								log.Error().Msg("type assertion failed")
								l.Remove(e)
								e = next
								continue
							}
							if vars.Name == name {
								ctx, cancel := context.WithTimeout(context.Background(), time.Second)
								defer cancel()
								r, err := matchClient.Balances(ctx, &pb.BalancesRequest{
									UserId:   sub,
									Currency: vars.Name,
								})
								if err != nil {
									// TODO: return error message
									log.Error().Msg("failed to get balance")
									continue
								}
								var rtc Message
								if len(r.Balances) != 1 {
									log.Error().Int("len", len(r.Balances)).Msg("wrong length of asset array")
									continue
								}
								rtc.Type = "asset"
								rtc.Balance = r.Balances[0].Available + r.Balances[0].Hold
								rtc.Available = r.Balances[0].Available
								rtc.Hold = r.Balances[0].Hold
								rtc.ProductId = vars.Name
								b, err := json.Marshal(rtc)
								if err != nil {
									log.Error().Msg("marshal reply")
									continue
								}
								err = vars.Conn.WriteMessage(websocket.TextMessage, b)
								if err != nil {
									log.Warn().Msg("failed to write message")
									l.Remove(e)
								}
							}
							e = next
						}
					}
				case "orders":
					log.Info().Str("key", string(e.Key)).Str("value", string(e.Value)).Msg("receive order message")
					var msg orderMessage
					err = json.Unmarshal(e.Value, &msg)
					if err != nil {
						log.Error().Msg("unmarshal order message")
						continue
					}

					if msg.Event == 0 {
						continue
					}

					var uo userOrder
					err = json.Unmarshal(msg.Order, &uo)
					if err != nil {
						log.Error().Msg("unmarshal user order")
						continue
					}

					l, ok := asset[uo.User]
					if ok {
						for e := l.Front(); e != nil; {
							next := e.Next()
							vars, ok := e.Value.(assetVars)
							if !ok {
								log.Error().Msg("type assertion failed")
								l.Remove(e)
								e = next
								continue
							}

							if vars.Name == msg.Stock {
								ctx, cancel := context.WithTimeout(context.Background(), time.Second)
								defer cancel()
								r, err := matchClient.Balances(ctx, &pb.BalancesRequest{
									UserId:   uo.User,
									Currency: vars.Name,
								})
								if err != nil {
									// TODO: return error message
									log.Error().Msg("failed to get balance")
									continue
								}
								var rtc Message
								if len(r.Balances) != 1 {
									log.Error().Int("len", len(r.Balances)).Msg("wrong length of asset array")
									continue
								}
								rtc.Type = "asset"
								rtc.Balance = r.Balances[0].Available + r.Balances[0].Hold
								rtc.Available = r.Balances[0].Available
								rtc.Hold = r.Balances[0].Hold
								rtc.ProductId = vars.Name
								b, err := json.Marshal(rtc)
								if err != nil {
									log.Error().Msg("marshal reply")
									continue
								}
								err = vars.Conn.WriteMessage(websocket.TextMessage, b)
								if err != nil {
									log.Warn().Msg("failed to write message")
									l.Remove(e)
								}
							} else if vars.Name == msg.Money {
								ctx, cancel := context.WithTimeout(context.Background(), time.Second)
								defer cancel()
								r, err := matchClient.Balances(ctx, &pb.BalancesRequest{
									UserId:   uo.User,
									Currency: vars.Name,
								})
								if err != nil {
									// TODO: return error message
									log.Error().Msg("failed to get balance")
									continue
								}
								var rtc Message
								if len(r.Balances) != 1 {
									log.Error().Int("len", len(r.Balances)).Msg("wrong length of asset array")
									continue
								}
								rtc.Type = "asset"
								rtc.Balance = r.Balances[0].Available + r.Balances[0].Hold
								rtc.Available = r.Balances[0].Available
								rtc.Hold = r.Balances[0].Hold
								rtc.ProductId = vars.Name
								b, err := json.Marshal(rtc)
								if err != nil {
									log.Error().Msg("marshal reply")
									continue
								}
								err = vars.Conn.WriteMessage(websocket.TextMessage, b)
								if err != nil {
									log.Error().Msg("failed to write message")
									l.Remove(e)
								}
							}
							e = next
						}
					}

					l, ok = order[uo.User]
					if ok {
						event := strconv.FormatInt(msg.Event, 10)
						params := json.RawMessage(`[` + event + `,`)
						params = append(params, msg.Order...)
						params = append(params, json.RawMessage(`]`)...)
						rtc := rpcToClient{
							Id:     json.RawMessage(`0`),
							Method: "order.update",
							Params: params,
						}
						b, err := json.Marshal(rtc)
						if err != nil {
							log.Error().Msg("marshal reply")
							continue
						}
						for e := l.Front(); e != nil; {
							next := e.Next()
							vars, ok := e.Value.(orderVars)
							if ok {
								fields := strings.Split(vars.ProductId, "-")
								productId := strings.Join(fields, "")
								if productId == uo.Market {
									err = vars.Conn.WriteMessage(websocket.TextMessage, b)
									if err != nil {
										log.Warn().Msg("failed to write message")
										l.Remove(e)
									}
								}
							} else {
								log.Error().Msg("type assertion failed")
								l.Remove(e)
							}
							e = next
						}
					}
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

	fmt.Printf("Closing consumer\n")
	consumer.Close()
}

func main() {
	log.Info().Msg("Starting siren-ws")

	viper.SetEnvPrefix("ws")
	viper.AutomaticEnv()
	replacer := strings.NewReplacer("-", "_")
	viper.SetEnvKeyReplacer(replacer)

	{
		conn, err := grpc.Dial("siren-match:50051", grpc.WithInsecure())
		if err != nil {
			log.Panic().Msgf("did not connect: %v", err)
		}
		defer conn.Close()
		matchClient = pb.NewMatchClient(conn)
		log.Info().Msg("connected to siren-match:50051")
	}
	{
		conn, err := grpc.Dial("siren-memorystore:50051", grpc.WithInsecure())
		if err != nil {
			log.Panic().Msgf("did not connect: %v", err)
		}
		defer conn.Close()
		memorystoreClient = pb.NewMemorystoreClient(conn)
		log.Info().Msg("connected to siren-memorystore:50051")
	}

	kline = make(map[string]*list.List)
	depth = make(map[string]*list.List)
	today = make(map[string]*list.List)
	deals = make(map[string]dealsVars)
	order = make(map[string]*list.List)
	asset = make(map[string]*list.List)

	// interval := 500 * time.Millisecond
	interval := 5000 * time.Millisecond
	go metimer(interval)
	go mptimer(interval)
	go handleMessages()

	r := mux.NewRouter()
	r.HandleFunc("/healthz", func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{}`)
	}).Methods("GET")
	r.HandleFunc("/", home)

	n := negroni.New()
	n.Use(negroni.NewRecovery())
	n.UseHandler(r)

	n.Run(":3001")
}
