package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/shopspring/decimal"

	"github.com/en/siren/utils"
	log "github.com/en/siren/utils/glog"
)

type HistoryType int32

const (
	HISTORY_TYPE_USER_BALANCE HistoryType = 0
	HISTORY_TYPE_USER_ORDER   HistoryType = 1
	HISTORY_TYPE_USER_DEAL    HistoryType = 2
	HISTORY_TYPE_ORDER_DETAIL HistoryType = 3
	HISTORY_TYPE_ORDER_DEAL   HistoryType = 4
)

var history map[HistoryType]string

func runCleanHistory() {
	timer := time.After(1000 * time.Millisecond)
	for {
		select {
		case <-timer:
			timer = time.After(1000 * time.Millisecond)
			if len(history) == 0 {
				continue
			}
			for k, sql := range history {
				fmt.Println("sql:", sql)
				delete(history, k)
			}
		}
	}
}

func appendUserOrder(o *order) {
	var (
		sql string
		ok  bool
	)
	sql, ok = history[HISTORY_TYPE_USER_ORDER]
	if ok {
		sql = sql + ", "
	} else {
		sql = "INSERT INTO `order_history` (id, `create_time`, `finish_time`, `user`, `market`, `t`, `side`, `price`, `amount`, `taker_fee`, `maker_fee`, `deal_stock`, `deal_money`, `deal_fee`) VALUES "
	}

	sql = sql + fmt.Sprintf("('%s', %d, %d, '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')", o.Id, utils.UnixMilli(o.CreatedAt), utils.UnixMilli(o.UpdatedAt), o.UserId, o.Symbol, o.Type, o.Side, o.Price, o.Amount, o.TakerFee, o.MakerFee, o.FilledAmount, o.ExecutedValue, o.FillFees)
	history[HISTORY_TYPE_USER_ORDER] = sql
}

func appendOrderDetail(o *order) {
	var (
		sql string
		ok  bool
	)
	sql, ok = history[HISTORY_TYPE_ORDER_DETAIL]
	if ok {
		sql = sql + ", "
	} else {
		sql = "INSERT INTO `order_detail` (`id`, `create_time`, `finish_time`, `user`, `market`, `t`, `side`, `price`, `amount`, `taker_fee`, `maker_fee`, `deal_stock`, `deal_money`, `deal_fee`) VALUES "
	}

	sql = sql + fmt.Sprintf("('%s', %d, %d, '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')", o.Id, utils.UnixMilli(o.CreatedAt), utils.UnixMilli(o.UpdatedAt), o.UserId, o.Symbol, o.Type, o.Side, o.Price, o.Amount, o.TakerFee, o.MakerFee, o.FilledAmount, o.ExecutedValue, o.FillFees)
	history[HISTORY_TYPE_ORDER_DETAIL] = sql
}

func appendOrderDeal(t int64, userId string, dealId string, orderId string, dealOrderId string, role int32, price decimal.Decimal, amount decimal.Decimal, deal decimal.Decimal, fee decimal.Decimal, dealFee decimal.Decimal) {
	var (
		sql string
		ok  bool
	)
	sql, ok = history[HISTORY_TYPE_ORDER_DEAL]
	if ok {
		sql = sql + ", "
	} else {
		sql = "INSERT INTO `deal_history` (`id`, `time`, `user`, `deal_id`, `order_id`, `deal_order_id`, `role`, `price`, `amount`, `deal`, `fee`, `deal_fee`) VALUES "
	}

	sql = sql + fmt.Sprintf("(NULL, %d, '%s', '%s', '%s', %d, '%s', '%s', '%s', '%s', '%s', '%s')", t, userId, dealId, orderId, dealOrderId, role, price.String(), amount.String(), deal.String(), fee.String(), dealFee.String())
	history[HISTORY_TYPE_ORDER_DEAL] = sql
}

func appendUserDeal(t int64, userId string, market string, dealId string, orderId string, dealOrderId string, side string, role int32, price decimal.Decimal, amount decimal.Decimal, deal decimal.Decimal, fee decimal.Decimal, dealFee decimal.Decimal) {
	var (
		sql string
		ok  bool
	)
	sql, ok = history[HISTORY_TYPE_USER_DEAL]
	if ok {
		sql = sql + ", "
	} else {
		sql = sql + "INSERT INTO `user_deal_history` (`id`, `time`, `user`, `market`, `deal_id`, `order_id`, `deal_order_id`, `side`, `role`, `price`, `amount`, `deal`, `fee`, `deal_fee`) VALUES "
	}

	sql = sql + fmt.Sprintf("(NULL, %d, '%s', '%s', '%s', '%s', '%s', '%s', %d, '%s', '%s', '%s', '%s', '%s')", t, userId, market, dealId, orderId, dealOrderId, side, role, price.String(), amount.String(), deal.String(), fee.String(), dealFee.String())
	history[HISTORY_TYPE_USER_DEAL] = sql
}

func appendUserBalance(t int64, userId string, asset string, business string, change decimal.Decimal, balance decimal.Decimal, detail string) {
	var (
		sql string
		ok  bool
	)
	sql, ok = history[HISTORY_TYPE_USER_BALANCE]
	if ok {
		sql = sql + ", "
	} else {
		sql = sql + "INSERT INTO `balance_history` (`id`, `time`, `user`, `asset`, `business`, `change`, `balance`, `detail`) VALUES "
	}

	sql = sql + fmt.Sprintf("(NULL, %d, '%s', '%s', '%s', '%s', '%s', '%s')", t, userId, asset, business, change.String(), balance.String(), detail)
	history[HISTORY_TYPE_USER_BALANCE] = sql
}

func appendOrderHistory(o *order) {
	appendUserOrder(o)
	appendOrderDetail(o)
}

func appendOrderDealHistory(t int64, dealId string, ask *order, askRole int32, bid *order, bidRole int32, price decimal.Decimal, amount decimal.Decimal, deal decimal.Decimal, askFee decimal.Decimal, bidFee decimal.Decimal) {
	appendOrderDeal(t, ask.UserId, dealId, ask.Id, bid.Id, askRole, price, amount, deal, askFee, bidFee)
	appendOrderDeal(t, bid.UserId, dealId, bid.Id, ask.Id, bidRole, price, amount, deal, bidFee, askFee)
	appendUserDeal(t, ask.UserId, ask.Symbol, dealId, ask.Id, bid.Id, ask.Side, askRole, price, amount, deal, askFee, bidFee)
	appendUserDeal(t, bid.UserId, ask.Symbol, dealId, bid.Id, ask.Id, bid.Side, bidRole, price, amount, deal, bidFee, askFee)
}

func appendUserBalanceHistory(t int64, userId string, asset string, business string, change decimal.Decimal, detail string) {
	balance := balanceGet(asset, userId, ACCOUNT_TYPE_CASH)
	appendUserBalance(t, userId, asset, business, change, balance.available.Add(balance.hold), detail)
	if business == "Trade" {
		key := fmt.Sprintf("%s:cash:%s", userId, asset)
		k := Kb{
			RefId:     "TODO:trade_id",
			Type:      "Trade",
			Available: balance.available.String(),
			Hold:      balance.hold.String(),
		}
		b, err := json.Marshal(k)
		if err != nil {
			log.Error().Err(err).Msg("failed to marshal kafka balance message")
			return
		}
		producer.ProduceChannel() <- &kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &topicSirenBalances, Partition: kafka.PartitionAny}, Value: b, Key: []byte(key)}
	}
}
