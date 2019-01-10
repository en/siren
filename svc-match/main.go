package main

import (
	"container/list"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/golang/protobuf/ptypes"
	_ "github.com/lib/pq"
	"github.com/shopspring/decimal"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/en/siren/messaging"
	pb "github.com/en/siren/protos"
	skiplist "github.com/en/siren/svc-match/fast-skiplist"
	log "github.com/en/siren/utils/glog"
)

const (
	port = ":50051"
)

var db *sql.DB

type server struct{}

func (s *server) GetCurrencies(ctx context.Context, in *pb.EmptyRequest) (*pb.GetCurrencyResponse, error) {
	response := new(pb.GetCurrencyResponse)
	for _, v := range currencies {
		c := new(pb.Currency)
		c.Id = v.id
		c.Name = v.name
		response.Currencies = append(response.Currencies, c)
	}
	return response, nil
}

func (s *server) GetSymbols(ctx context.Context, in *pb.EmptyRequest) (*pb.GetSymbolsResponse, error) {
	response := new(pb.GetSymbolsResponse)
	for _, v := range currencyPairs {
		cp := new(pb.Symbol)
		cp.Id = v.id
		cp.BaseCurrency = v.baseCurrency
		cp.QuoteCurrency = v.quoteCurrency
		cp.BaseMinAmount = v.baseMinAmount
		cp.BaseIncrement = v.baseIncrement
		cp.QuoteIncrement = v.quoteIncrement
		response.Symbols = append(response.Symbols, cp)
	}
	return response, nil
}

func (s *server) Balances(ctx context.Context, in *pb.BalancesRequest) (*pb.BalancesResponse, error) {
	response := new(pb.BalancesResponse)
	if in.Currency != "" {
		assets.RLock()
		u, ok := assets.users[in.UserId]
		assets.RUnlock()
		b := new(pb.Balance)
		b.Currency = in.Currency

		if ok {
			u.RLock()
			account := u.cash[in.Currency]
			u.RUnlock()
			b.Balance = account.available.Add(account.hold).StringFixedBank(8)
			b.Available = account.available.StringFixedBank(8)
			b.Hold = account.hold.StringFixedBank(8)
		} else {
			b.Balance = decimal.Zero.StringFixedBank(8)
			b.Available = decimal.Zero.StringFixedBank(8)
			b.Hold = decimal.Zero.StringFixedBank(8)
		}
		response.Balances = append(response.Balances, b)
		return response, nil
	}
	assets.RLock()
	u, ok := assets.users[in.UserId]
	assets.RUnlock()

	if ok {
		u.RLock()
		for c, a := range u.cash {
			b := new(pb.Balance)
			b.Currency = c
			b.Balance = a.available.Add(a.hold).StringFixedBank(8)
			b.Available = a.available.StringFixedBank(8)
			b.Hold = a.hold.StringFixedBank(8)
			response.Balances = append(response.Balances, b)
		}
		u.RUnlock()
	}
	return response, nil
}

func (s *server) GetAssetSummary(ctx context.Context, in *pb.GetAssetSummaryRequest) (*pb.GetAssetSummaryResponse, error) {
	response := new(pb.GetAssetSummaryResponse)
	if len(in.Currencies) == 0 {
		for k := range currencies {
			a := new(pb.Asset)
			a.Currency = k
			status := balanceStatus(k, ACCOUNT_TYPE_CASH)
			a.Total = status.total.String()
			a.Available = status.available.String()
			a.AvailableCount = status.availableCount
			a.Hold = status.hold.String()
			a.HoldCount = status.holdCount
			response.Assets = append(response.Assets, a)
		}
	} else {
		for _, id := range in.Currencies {
			a := new(pb.Asset)
			a.Currency = id
			status := balanceStatus(id, ACCOUNT_TYPE_CASH)
			a.Total = status.total.String()
			a.Available = status.available.String()
			a.AvailableCount = status.availableCount
			a.Hold = status.hold.String()
			a.HoldCount = status.holdCount
			response.Assets = append(response.Assets, a)
		}
	}
	return response, nil
}

// TODO: recover
func (s *server) NewOrder(ctx context.Context, in *pb.NewOrderRequest) (*pb.NewOrderResponse, error) {
	response := new(pb.NewOrderResponse)
	response.Order = new(pb.Order)
	market, ok := currencyPairs[in.Symbol]
	if ok {
		var order *order
		if in.Type == "limit" {
			order = marketPutLimitOrder(true, market, in.UserId, in.Side, in.Amount, in.Price)
			// TODO: if not err
			// appendOperlog("limit_order", []interface{}{in.UserId, in.Symbol, in.Side, in.Amount, in.Price, in.TakerFee, in.MakerFee})
		} else if in.Type == "market" {
			order = marketPutMarketOrder(true, market, in.UserId, in.Side, in.Amount)
			// TODO: if not err
			// appendOperlog("market_order", []interface{}{in.UserId, in.Symbol, in.Side, in.Amount, in.TakerFee})
		} else {
			// move to gateway
			// TODO: error
		}
		if order != nil {
			createdAt, err := ptypes.TimestampProto(order.CreatedAt)
			if err != nil {
				log.Error().Msg("timestamp convert failed")
				return response, err
			}
			updatedAt, err := ptypes.TimestampProto(order.UpdatedAt)
			if err != nil {
				log.Error().Msg("timestamp convert failed")
				return response, err
			}
			doneAt, err := ptypes.TimestampProto(order.DoneAt)
			if err != nil {
				log.Error().Msg("timestamp convert failed")
				return response, err
			}

			response.Order.Id = order.Id
			response.Order.Price = order.Price.String()
			response.Order.Amount = order.Amount.String()
			response.Order.Symbol = order.Symbol
			response.Order.Side = order.Side
			response.Order.Type = order.Type
			response.Order.CreatedAt = createdAt
			response.Order.UpdatedAt = updatedAt
			response.Order.DoneAt = doneAt
			response.Order.DoneReason = order.DoneReason
			response.Order.FillFees = order.FillFees.String()
			response.Order.FilledAmount = order.FilledAmount.String()
			response.Order.RemainingAmount = order.RemainingAmount.String()
			response.Order.ExecutedValue = order.ExecutedValue.String()
			response.Order.Status = order.Status
		}
	}
	return response, nil
}

func (s *server) Orders(ctx context.Context, in *pb.OrdersRequest) (*pb.OrdersResponse, error) {
	response := new(pb.OrdersResponse)
	market, ok := currencyPairs[in.Symbol]
	if ok {
		if orders, ok := market.ordersByUser[in.UserId]; ok {
			for e := orders.Front(); e != nil; e = e.Next() {
				id := e.Value.(string)
				if o, ok := market.orders[id]; ok {
					createdAt, err := ptypes.TimestampProto(o.CreatedAt)
					if err != nil {
						log.Error().Msg("timestamp convert failed")
						return response, err
					}
					updatedAt, err := ptypes.TimestampProto(o.UpdatedAt)
					if err != nil {
						log.Error().Msg("timestamp convert failed")
						return response, err
					}
					doneAt, err := ptypes.TimestampProto(o.DoneAt)
					if err != nil {
						log.Error().Msg("timestamp convert failed")
						return response, err
					}
					order := new(pb.Order)
					order.Id = id
					order.Price = o.Price.String()
					order.Amount = o.Amount.String()
					order.Symbol = o.Symbol
					order.Side = o.Side
					order.Type = o.Type
					order.CreatedAt = createdAt
					order.UpdatedAt = updatedAt
					order.DoneAt = doneAt
					order.DoneReason = o.DoneReason
					order.FillFees = o.FillFees.String()
					order.FilledAmount = o.FilledAmount.String()
					order.RemainingAmount = o.RemainingAmount.String()
					order.ExecutedValue = o.ExecutedValue.String()
					order.Status = o.Status
					response.Orders = append(response.Orders, order)
				} else {
					log.Error().Str("order_id", id).Msg("order not found")
				}
			}
		}
	}
	return response, nil
}

func (s *server) OrderCancel(ctx context.Context, in *pb.OrderCancelRequest) (*pb.OrderCancelResponse, error) {
	response := new(pb.OrderCancelResponse)
	market, ok := currencyPairs[in.Symbol]
	if ok {
		order, ok := market.orders[in.OrderId]
		if ok {
			// TODO: check order.UserId == in.UserId
			marketCancelOrder(true, market, order)
			// response.Order.Id = order.Id
			// response.Order.Type = order.Type
			// response.Order.Side = order.Side
			// response.Order.CreateTime = order.CreateTime
			// response.Order.UpdateTime = order.UpdateTime
			// response.Order.UserId = order.UserId
			// response.Order.Market = order.Market
			// response.Order.Price = order.Price.String()
			// response.Order.Amount = order.Amount.String()
			// response.Order.TakerFee = order.TakerFee.String()
			// response.Order.MakerFee = order.MakerFee.String()
			// response.Order.Left = order.Left.String()
			// response.Order.DealStock = order.DealStock.String()
			// response.Order.DealMoney = order.DealMoney.String()
			// response.Order.DealFee = order.DealFee.String()

			// appendOperlog("cancel_order", []interface{}{in.UserId, in.Symbol, in.OrderId})
		}
	}
	return response, nil
}

func (s *server) OrderBook(ctx context.Context, in *pb.OrderBookRequest) (*pb.OrderBookResponse, error) {
	response := new(pb.OrderBookResponse)
	market, ok := currencyPairs[in.Symbol]
	if ok {
		var e *skiplist.Element
		if in.Side == 1 {
			e = market.asks.Front()
		} else {
			e = market.bids.Front()
		}
		count := uint64(0)
		for ; e != nil; e = e.Next() {
			if count < in.Limit {
				count = count + 1
				// o := e.Value().(*order)
				o1 := new(pb.Order)
				// o1.Id = o.Id
				// o1.Type = o.Type
				// o1.Side = o.Side
				// o1.CreateTime = o.CreateTime
				// o1.UpdateTime = o.UpdateTime
				// o1.UserId = o.UserId
				// o1.Market = o.Market
				// o1.Price = o.Price.String()
				// o1.Amount = o.Amount.String()
				// o1.TakerFee = o.TakerFee.String()
				// o1.MakerFee = o.MakerFee.String()
				// o1.Left = o.Left.String()
				// o1.DealStock = o.DealStock.String()
				// o1.DealMoney = o.DealMoney.String()
				// o1.DealFee = o.DealFee.String()
				response.Orders = append(response.Orders, o1)
			}
		}

	}
	return response, nil
}

func (s *server) OrderBookDepth(ctx context.Context, in *pb.OrderBookDepthRequest) (*pb.OrderBookDepthResponse, error) {
	response := new(pb.OrderBookDepthResponse)
	market, ok := currencyPairs[in.Symbol]
	if ok {
		count := uint64(0)
		for e := market.asks.Front(); e != nil; {
			if count >= in.Limit {
				break
			}
			count = count + 1
			o := e.Value().(*order)
			data := new(pb.OrderBookData)
			price := o.Price
			quantity := o.RemainingAmount
			for e = e.Next(); e != nil; e = e.Next() {
				o1 := e.Value().(*order)
				if o1.Price.Equal(price) {
					quantity = quantity.Add(o1.RemainingAmount)
				} else {
					break
				}
			}
			data.Price = price.String()
			data.Quantity = quantity.String()
			response.Asks = append(response.Asks, data)
		}

		count = uint64(0)
		for e := market.bids.Front(); e != nil; {
			if count >= in.Limit {
				break
			}
			count = count + 1
			o := e.Value().(*order)
			data := new(pb.OrderBookData)
			price := o.Price
			quantity := o.RemainingAmount
			for e = e.Next(); e != nil; e = e.Next() {
				o1 := e.Value().(*order)
				if o1.Price.Equal(price) {
					quantity = quantity.Add(o1.RemainingAmount)
				} else {
					break
				}
			}
			data.Price = price.String()
			data.Quantity = quantity.String()
			response.Asks = append(response.Asks, data)
		}
	}
	return response, nil
}

func (s *server) OrderDetail(ctx context.Context, in *pb.OrderDetailRequest) (*pb.OrderDetailResponse, error) {
	response := new(pb.OrderDetailResponse)
	market, ok := currencyPairs[in.Symbol]
	if ok {
		order, ok := market.orders[in.OrderId]
		if ok {
			response.Order.Id = order.Id
			// response.Order.Type = order.Type
			// response.Order.Side = order.Side
			// response.Order.CreateTime = order.CreateTime
			// response.Order.UpdateTime = order.UpdateTime
			// response.Order.UserId = order.UserId
			// response.Order.Market = order.Market
			// response.Order.Price = order.Price.String()
			// response.Order.Amount = order.Amount.String()
			// response.Order.TakerFee = order.TakerFee.String()
			// response.Order.MakerFee = order.MakerFee.String()
			// response.Order.Left = order.Left.String()
			// response.Order.DealStock = order.DealStock.String()
			// response.Order.DealMoney = order.DealMoney.String()
			// response.Order.DealFee = order.DealFee.String()
		}
	}
	return response, nil
}

func (s *server) GetMarketSummary(ctx context.Context, in *pb.GetMarketSummaryRequest) (*pb.GetMarketSummaryResponse, error) {
	response := new(pb.GetMarketSummaryResponse)
	if len(in.Symbols) == 0 {
		for id, v := range currencyPairs {
			m := new(pb.Market)
			m.Symbol = id
			// TODO: check this
			m.AsksCount, m.AsksAmount, m.BidsCount, m.BidsAmount = marketGetStatus(v)
			response.Markets = append(response.Markets, m)
		}
	} else {
		for _, id := range in.Symbols {
			m := new(pb.Market)
			m.Symbol = id
			if cp, ok := currencyPairs[id]; ok {
				m.AsksCount, m.AsksAmount, m.BidsCount, m.BidsAmount = marketGetStatus(cp)
			}
			response.Markets = append(response.Markets, m)
		}
	}
	return response, nil
}

func (s *server) UpdateBalance(ctx context.Context, in *pb.UpdateBalanceRequest) (*pb.UpdateBalanceResponse, error) {
	response := new(pb.UpdateBalanceResponse)
	amount, err := decimal.NewFromString(in.Amount)
	if err != nil {
		log.Error().Str("amount", in.Amount).Msg("wrong decimal")
		return response, nil
	}
	updateUserBalance(true, in.Id, in.Type, in.UserId, in.Currency, ACCOUNT_TYPE_CASH, amount)
	// appendOperlog("update_balance", []interface{}{in.Id, in.Type, in.UserId, in.Currency, amount})
	key := fmt.Sprintf("%s:cash:%s", in.UserId, in.Currency)
	balance := balanceGet(in.Currency, in.UserId, ACCOUNT_TYPE_CASH)
	k := Kb{
		RefId:     in.Id,
		Type:      "Transfer",
		Available: balance.available.String(),
		Hold:      balance.hold.String(),
	}
	b, err := json.Marshal(k)
	if err != nil {
		log.Error().Err(err).Msg("failed to marshal kafka balance message")
	}
	producer.ProduceChannel() <- &kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &topicSirenBalances, Partition: kafka.PartitionAny}, Value: b, Key: []byte(key)}

	transfer := messaging.TransferMessage{
		Id:        in.Id,
		UserId:    in.UserId,
		Currency:  in.Currency,
		Type:      in.Type,
		Amount:    amount,
		Status:    "",
		CreatedAt: time.Now(),
	}
	b, err = json.Marshal(transfer)
	if err != nil {
		log.Error().Err(err).Msg("failed to marshal kafka transfer message")
	}
	producer.ProduceChannel() <- &kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &messaging.TopicSirenTransfers, Partition: kafka.PartitionAny}, Value: b}
	return response, nil
}

func loadLimitOrder(params []interface{}) {
	userId := params[0].(string)
	market := params[1].(string)
	side := params[2].(string)
	amount := params[3].(string)
	price := params[4].(string)
	takerFee := params[5].(string)
	makerFee := params[6].(string)
	fmt.Println("load_limit_order, user_id:", userId, "market:", market, "side:", side, "amount:", amount, "price:", price, "taker_fee:", takerFee, "maker_fee:", makerFee)
	// source
	m, ok := currencyPairs[market]
	if ok {
		marketPutLimitOrder(false, m, userId, side, amount, price)
	} else {
		log.Error().Str("currency_pair", market).Msg("market not exists")
	}
}

func loadMarketOrder(params []interface{}) {
	userId := params[0].(string)
	market := params[1].(string)
	side := params[2].(string)
	amount := params[3].(string)
	takerFee := params[4].(string)
	fmt.Println("load_market_order, user_id:", userId, "market:", market, "side:", side, "amount:", amount, "taker_fee:", takerFee)
	// source
	m, ok := currencyPairs[market]
	if ok {
		marketPutMarketOrder(false, m, userId, side, amount)
	} else {
		log.Error().Str("currency_pair", market).Msg("market not exists")
	}
}

func loadCancelOrder(params []interface{}) {
	userId := params[0].(string)
	market := params[1].(string)
	orderId := params[2].(string)
	fmt.Println("load_cancel_order, user_id:", userId, "market:", market, "order_id:", orderId)
	m, ok := currencyPairs[market]
	if ok {
		order, ok := m.orders[orderId]
		if ok {
			marketCancelOrder(false, m, order)
		} else {
			log.Error().Str("order_id", orderId).Msg("order not found")
		}
	} else {
		log.Error().Str("currency_pair", market).Msg("market not exists")
	}
}

func main() {
	log.Info().Msg("Starting siren-match")

	viper.SetEnvPrefix("match")
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

	// init_balance
	rows, err := db.QueryContext(ctx, "SELECT id, name, min_size FROM currencies")
	if err != nil {
		log.Fatal().Err(err).Msg("")
	}
	defer rows.Close()

	assets = asset{users: make(map[string]*user)}
	currencies = make(map[string]currency)
	for rows.Next() {
		var c currency
		if err := rows.Scan(&c.id, &c.name, &c.minSize); err != nil {
			log.Fatal().Err(err).Msg("")
		}
		symbol := strings.ToUpper(c.id)
		currencies[symbol] = c
	}
	if err := rows.Err(); err != nil {
		log.Fatal().Err(err).Msg("")
	}
	fmt.Println(currencies)

	// init_trade
	currencyPairs = make(map[string]*currencyPair)
	rows, err = db.QueryContext(ctx, "SELECT id, base_currency, quote_currency, base_min_size, base_max_size, base_increment, quote_increment FROM symbols")
	if err != nil {
		log.Fatal().Err(err).Msg("")
	}
	defer rows.Close()

	for rows.Next() {
		cp := new(currencyPair)
		if err := rows.Scan(&cp.id, &cp.baseCurrency, &cp.quoteCurrency, &cp.baseMinAmount, &cp.baseMaxAmount, &cp.baseIncrement, &cp.quoteIncrement); err != nil {
			log.Fatal().Err(err).Msg("")
		}

		cp.asks = skiplist.New(true)
		cp.bids = skiplist.New(false)
		cp.orders = make(map[string]*order)
		cp.ordersByUser = make(map[string]*list.List)
		cp.takerFee, _ = decimal.NewFromString("0.002")
		cp.makerFee, _ = decimal.NewFromString("0.002")
		currencyPairs[cp.id] = cp
	}
	if err := rows.Err(); err != nil {
		log.Fatal().Err(err).Msg("")
	}
	fmt.Println(currencyPairs)

	// init_update

	// init_from_db
	loadFromKafka()
	// init_from_db end

	// init_operlog

	// init_history
	history = make(map[HistoryType]string)
	go runCleanHistory()
	// init_message
	initMessage()
	defer producer.Close()
	// init_persist
	// init_cli
	// init_server

	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to listen")
	}
	s := grpc.NewServer()
	// TODO
	pb.RegisterMatchServer(s, &server{})
	reflection.Register(s)
	if err := s.Serve(lis); err != nil {
		log.Fatal().Msg("failed to serve")
	}
}
