package main

import (
	"container/list"
	"encoding/json"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/gofrs/uuid"
	"github.com/shopspring/decimal"

	skiplist "github.com/en/siren/svc-match/fast-skiplist"
	"github.com/en/siren/utils"
	log "github.com/en/siren/utils/glog"
)

const (
	MARKET_ORDER_SIDE_ASK = 1
	MARKET_ORDER_SIDE_BID = 2

	MARKET_ROLE_MAKER = 1
	MARKET_ROLE_TAKER = 2
)

type order struct {
	UserId string `json:"user_id"`

	Freeze decimal.Decimal `json:"freeze"`

	Id              string          `json:"id"`
	Price           decimal.Decimal `json:"price"`
	Amount          decimal.Decimal `json:"amount"`
	Symbol          string          `json:"symbol"`
	Side            string          `json:"side"`
	Type            string          `json:"type"`
	CreatedAt       time.Time       `json:"created_at"`
	UpdatedAt       time.Time       `json:"updated_at"`
	DoneAt          time.Time       `json:"done_at"`
	DoneReason      string          `json:"done_reason"`
	FillFees        decimal.Decimal `json:"fill_fees"`
	FilledAmount    decimal.Decimal `json:"filled_amount"`
	RemainingAmount decimal.Decimal `json:"remaining_amount"`
	ExecutedValue   decimal.Decimal `json:"executed_value"`
	Status          string          `json:"status"`

	TakerFee decimal.Decimal `json:"taker_fee"`
	MakerFee decimal.Decimal `json:"maker_fee"`
}

type currencyPair struct {
	id             string `json:"id"`
	baseCurrency   string `json:"base_currency"`
	quoteCurrency  string `json:"quote_currency"`
	baseMinAmount  string `json:"base_min_amount"`
	baseMaxAmount  string `json:"base_min_amount"`
	baseIncrement  string `json:"base_Increment"`
	quoteIncrement string `json:"quote_increment"`

	takerFee decimal.Decimal
	makerFee decimal.Decimal

	asks *skiplist.SkipList
	bids *skiplist.SkipList

	// order_id -> *order
	orders map[string]*order
	// user_id -> list of order_id
	ordersByUser map[string]*list.List
}

var currencyPairs map[string]*currencyPair

type HistoryDetail struct {
	M string `json:"m"`
	I string `json:"i"`
	P string `json:"p"`
	A string `json:"a"`
}

type ubh struct {
	Detail HistoryDetail `json:"detail"`
}

func appendBalanceTradeAdd(o *order, asset string, change decimal.Decimal, price decimal.Decimal, amount decimal.Decimal) {
	detail := ubh{
		Detail: HistoryDetail{
			M: o.Symbol,
			I: o.Id,
			P: price.String(),
			A: amount.String(),
		},
	}
	b, err := json.Marshal(detail)
	if err != nil {
		log.Error().Err(err).Msg("failed to marshal balance trade add message")
		return
	}

	appendUserBalanceHistory(utils.UnixMilli(o.UpdatedAt), o.UserId, asset, "Trade", change, string(b))
}

func appendBalanceTradeSub(o *order, asset string, change decimal.Decimal, price decimal.Decimal, amount decimal.Decimal) {
	detail := ubh{
		Detail: HistoryDetail{
			M: o.Symbol,
			I: o.Id,
			P: price.String(),
			A: amount.String(),
		},
	}
	b, err := json.Marshal(detail)
	if err != nil {
		log.Error().Err(err).Msg("failed to marshal balance trade sub message")
		return
	}

	appendUserBalanceHistory(utils.UnixMilli(o.UpdatedAt), o.UserId, asset, "Trade", change.Neg(), string(b))
}

func appendBalanceTradeFee(o *order, asset string, change decimal.Decimal, price decimal.Decimal, amount decimal.Decimal, feeRate decimal.Decimal) {
	type HistoryDetailWithFee struct {
		M string `json:"m"`
		I string `json:"i"`
		P string `json:"p"`
		A string `json:"a"`
		F string `json:"f"`
	}

	type ubh struct {
		Detail HistoryDetailWithFee `json:"detail"`
	}

	detail := ubh{
		Detail: HistoryDetailWithFee{
			M: o.Symbol,
			I: o.Id,
			P: price.String(),
			A: amount.String(),
			F: feeRate.String(),
		},
	}
	b, err := json.Marshal(detail)
	if err != nil {
		log.Error().Err(err).Msg("failed to marshal balance trade fee message")
		return
	}
	appendUserBalanceHistory(utils.UnixMilli(o.UpdatedAt), o.UserId, asset, "Trade", change.Neg(), string(b))
}

func executeLimitAskOrder(real bool, m *currencyPair, taker *order) {
	var (
		price         decimal.Decimal
		amount        decimal.Decimal
		executedValue decimal.Decimal
		askFee        decimal.Decimal
		bidFee        decimal.Decimal
	)

	for e := m.bids.Front(); e != nil; e = e.Next() {
		if taker.RemainingAmount.IsZero() {
			break
		}
		maker := e.Value().(*order)
		if taker.Price.GreaterThan(maker.Price) {
			break
		}

		price = maker.Price
		if taker.RemainingAmount.LessThan(maker.RemainingAmount) {
			amount = taker.RemainingAmount
		} else {
			amount = maker.RemainingAmount
		}

		executedValue = price.Mul(amount)
		askFee = executedValue.Mul(taker.TakerFee)
		bidFee = amount.Mul(maker.MakerFee)

		now := time.Now()
		taker.UpdatedAt = now
		maker.UpdatedAt = now
		id, err := uuid.NewV4()
		if err != nil {
			log.Error().Err(err).Msg("failed to generate UUID")
			break
		}
		dealId := id.String()
		if real {
			appendOrderDealHistory(utils.UnixMilli(taker.UpdatedAt), dealId, taker, MARKET_ROLE_TAKER, maker, MARKET_ROLE_MAKER, price, amount, executedValue, askFee, bidFee)
			pushDealMessage(utils.UnixMilli(taker.UpdatedAt), m.id, taker, maker, price, amount, askFee, bidFee, MARKET_ORDER_SIDE_ASK, dealId, m.baseCurrency, m.quoteCurrency)
		}

		taker.RemainingAmount = taker.RemainingAmount.Sub(amount)
		taker.FilledAmount = taker.FilledAmount.Add(amount)
		taker.ExecutedValue = taker.ExecutedValue.Add(executedValue)
		taker.FillFees = taker.FillFees.Add(askFee)

		balanceSub(m.baseCurrency, taker.UserId, ACCOUNT_TYPE_CASH, BALANCE_TYPE_AVAILABLE, amount)
		if real {
			appendBalanceTradeSub(taker, m.baseCurrency, amount, price, amount)
		}
		balanceAdd(m.quoteCurrency, taker.UserId, ACCOUNT_TYPE_CASH, BALANCE_TYPE_AVAILABLE, executedValue)
		if real {
			appendBalanceTradeAdd(taker, m.quoteCurrency, executedValue, price, amount)
		}

		if askFee.IsPositive() {
			balanceSub(m.quoteCurrency, taker.UserId, ACCOUNT_TYPE_CASH, BALANCE_TYPE_AVAILABLE, askFee)
			if real {
				appendBalanceTradeFee(taker, m.quoteCurrency, askFee, price, amount, taker.TakerFee)
			}
		}

		maker.RemainingAmount = maker.RemainingAmount.Sub(amount)
		maker.Freeze = maker.Freeze.Sub(executedValue)
		maker.FilledAmount = maker.FilledAmount.Add(amount)
		maker.ExecutedValue = maker.ExecutedValue.Add(executedValue)
		maker.FillFees = maker.FillFees.Add(bidFee)

		balanceSub(m.quoteCurrency, maker.UserId, ACCOUNT_TYPE_CASH, BALANCE_TYPE_HOLD, executedValue)
		if real {
			appendBalanceTradeSub(maker, m.quoteCurrency, executedValue, price, amount)
		}
		balanceAdd(m.baseCurrency, maker.UserId, ACCOUNT_TYPE_CASH, BALANCE_TYPE_AVAILABLE, amount)
		if real {
			appendBalanceTradeAdd(maker, m.baseCurrency, amount, price, amount)
		}

		if bidFee.IsPositive() {
			balanceSub(m.baseCurrency, maker.UserId, ACCOUNT_TYPE_CASH, BALANCE_TYPE_AVAILABLE, bidFee)
			if real {
				appendBalanceTradeFee(maker, m.baseCurrency, bidFee, price, amount, maker.MakerFee)
			}
		}

		if maker.RemainingAmount.IsZero() {
			if real {
				pushOrderMessage(ORDER_EVENT_TYPE_FINISH, maker, m.baseCurrency, m.quoteCurrency)
			}
			orderFinish(real, m, maker)
		} else {
			if real {
				pushOrderMessage(ORDER_EVENT_TYPE_UPDATE, maker, m.baseCurrency, m.quoteCurrency)
			}
		}
	}
}

func executeLimitBidOrder(real bool, m *currencyPair, taker *order) {
	var (
		price         decimal.Decimal
		amount        decimal.Decimal
		executedValue decimal.Decimal
		askFee        decimal.Decimal
		bidFee        decimal.Decimal
	)

	for e := m.asks.Front(); e != nil; e = e.Next() {
		if taker.RemainingAmount.IsZero() {
			break
		}

		maker := e.Value().(*order)
		if taker.Price.LessThan(maker.Price) {
			break
		}

		price = maker.Price
		if taker.RemainingAmount.LessThan(maker.RemainingAmount) {
			amount = taker.RemainingAmount
		} else {
			amount = maker.RemainingAmount
		}

		executedValue = price.Mul(amount)
		askFee = executedValue.Mul(maker.MakerFee)
		bidFee = amount.Mul(taker.TakerFee)

		now := time.Now()
		taker.UpdatedAt = now
		maker.UpdatedAt = now
		id, err := uuid.NewV4()
		if err != nil {
			log.Error().Err(err).Msg("failed to generate UUID")
			break
		}
		dealId := id.String()
		if real {
			appendOrderDealHistory(utils.UnixMilli(taker.UpdatedAt), dealId, maker, MARKET_ROLE_MAKER, taker, MARKET_ROLE_TAKER, price, amount, executedValue, askFee, bidFee)
			pushDealMessage(utils.UnixMilli(taker.UpdatedAt), m.id, maker, taker, price, amount, askFee, bidFee, MARKET_ORDER_SIDE_BID, dealId, m.baseCurrency, m.quoteCurrency)
		}

		taker.RemainingAmount = taker.RemainingAmount.Sub(amount)
		taker.FilledAmount = taker.FilledAmount.Add(amount)
		taker.ExecutedValue = taker.ExecutedValue.Add(executedValue)
		taker.FillFees = taker.FillFees.Add(bidFee)

		balanceSub(m.quoteCurrency, taker.UserId, ACCOUNT_TYPE_CASH, BALANCE_TYPE_AVAILABLE, executedValue)
		if real {
			appendBalanceTradeSub(taker, m.quoteCurrency, executedValue, price, amount)
		}
		balanceAdd(m.baseCurrency, taker.UserId, ACCOUNT_TYPE_CASH, BALANCE_TYPE_AVAILABLE, amount)
		if real {
			appendBalanceTradeAdd(taker, m.baseCurrency, amount, price, amount)
		}

		if bidFee.IsPositive() {
			balanceSub(m.baseCurrency, taker.UserId, ACCOUNT_TYPE_CASH, BALANCE_TYPE_AVAILABLE, bidFee)
			if real {
				appendBalanceTradeFee(taker, m.baseCurrency, bidFee, price, amount, taker.TakerFee)
			}
		}

		maker.RemainingAmount = maker.RemainingAmount.Sub(amount)
		maker.Freeze = maker.Freeze.Sub(amount)
		maker.FilledAmount = maker.FilledAmount.Add(amount)
		maker.ExecutedValue = maker.ExecutedValue.Add(executedValue)
		maker.FillFees = maker.FillFees.Add(askFee)

		balanceSub(m.baseCurrency, maker.UserId, ACCOUNT_TYPE_CASH, BALANCE_TYPE_HOLD, amount)
		if real {
			appendBalanceTradeSub(maker, m.baseCurrency, amount, price, amount)
		}
		balanceAdd(m.quoteCurrency, maker.UserId, ACCOUNT_TYPE_CASH, BALANCE_TYPE_AVAILABLE, executedValue)
		if real {
			appendBalanceTradeAdd(maker, m.quoteCurrency, executedValue, price, amount)
		}

		if askFee.IsPositive() {
			balanceSub(m.quoteCurrency, maker.UserId, ACCOUNT_TYPE_CASH, BALANCE_TYPE_AVAILABLE, askFee)
			if real {
				appendBalanceTradeFee(maker, m.quoteCurrency, askFee, price, amount, maker.MakerFee)
			}
		}

		if maker.RemainingAmount.IsZero() {
			if real {
				pushOrderMessage(ORDER_EVENT_TYPE_FINISH, maker, m.baseCurrency, m.quoteCurrency)
			}
			orderFinish(real, m, maker)
		} else {
			if real {
				pushOrderMessage(ORDER_EVENT_TYPE_UPDATE, maker, m.baseCurrency, m.quoteCurrency)
			}
		}
	}
}

func marketPutLimitOrder(real bool, m *currencyPair, userId string, side string, _amount string, _price string) *order {
	amount, err := decimal.NewFromString(_amount)
	if err != nil {
		log.Error().Str("amount", _amount).Msg("wrong decimal")
		return nil
	}
	price, err := decimal.NewFromString(_price)
	if err != nil {
		log.Error().Str("price", _price).Msg("wrong decimal")
		return nil
	}
	if side == "sell" {
		balance := balanceGet(m.baseCurrency, userId, ACCOUNT_TYPE_CASH).available
		if balance.LessThan(amount) {
			log.Debug().Str("avaiable", balance.String()).Str("require", _amount).Msg("not enough balance")
			// TODO: handle error
			return nil
		}
	} else { // "buy"
		balance := balanceGet(m.quoteCurrency, userId, ACCOUNT_TYPE_CASH).available
		require := amount.Mul(price)
		if balance.LessThan(require) {
			log.Debug().Str("avaiable", balance.String()).Str("require", require.String()).Msg("not enough balance")
			return nil
		}
	}
	// TODO move this check to gateway
	// if amount < m.fMinAmount {
	// 	return ""
	// }

	now := time.Now()
	o := new(order)
	o.UserId = userId
	id, err := uuid.NewV4()
	if err != nil {
		log.Error().Err(err).Msg("failed to generate UUID")
		return nil
	}
	o.Id = id.String()
	o.Price = price
	o.Amount = amount
	o.Symbol = m.id
	o.Side = side
	o.Type = "limit"
	o.CreatedAt = now
	o.UpdatedAt = now
	o.FillFees = decimal.Zero
	o.FilledAmount = decimal.Zero
	o.RemainingAmount = amount
	o.ExecutedValue = decimal.Zero
	o.TakerFee = m.takerFee
	o.MakerFee = m.makerFee

	o.Freeze = decimal.Zero

	if side == "sell" {
		executeLimitAskOrder(real, m, o)
	} else { // "buy"
		executeLimitBidOrder(real, m, o)
	}
	// TODO: handle error
	if o.RemainingAmount.IsZero() {
		if real {
			appendOrderHistory(o)
			pushOrderMessage(ORDER_EVENT_TYPE_FINISH, o, m.baseCurrency, m.quoteCurrency)
		}
	} else {
		if real {
			pushOrderMessage(ORDER_EVENT_TYPE_PUT, o, m.baseCurrency, m.quoteCurrency)
		}
		orderPut(true, m, o)
	}
	return o
}

func executeMarketAskOrder(real bool, m *currencyPair, taker *order) {
	var (
		price         decimal.Decimal
		amount        decimal.Decimal
		executedValue decimal.Decimal
		askFee        decimal.Decimal
		bidFee        decimal.Decimal
	)

	for e := m.bids.Front(); e != nil; e = e.Next() {
		if taker.RemainingAmount.IsZero() {
			break
		}

		maker := e.Value().(*order)
		price = maker.Price
		if taker.RemainingAmount.LessThan(maker.RemainingAmount) {
			amount = taker.RemainingAmount
		} else {
			amount = maker.RemainingAmount
		}

		executedValue = price.Mul(amount)
		askFee = executedValue.Mul(taker.TakerFee)
		bidFee = amount.Mul(maker.MakerFee)

		now := time.Now()
		taker.UpdatedAt = now
		maker.UpdatedAt = now
		id, err := uuid.NewV4()
		if err != nil {
			log.Error().Err(err).Msg("failed to generate UUID")
			break
		}
		dealId := id.String()
		if real {
			appendOrderDealHistory(utils.UnixMilli(taker.UpdatedAt), dealId, taker, MARKET_ROLE_TAKER, maker, MARKET_ROLE_MAKER, price, amount, executedValue, askFee, bidFee)
			pushDealMessage(utils.UnixMilli(taker.UpdatedAt), m.id, taker, maker, price, amount, askFee, bidFee, MARKET_ORDER_SIDE_ASK, dealId, m.baseCurrency, m.quoteCurrency)
		}

		taker.RemainingAmount = taker.RemainingAmount.Sub(amount)
		taker.FilledAmount = taker.FilledAmount.Add(amount)
		taker.ExecutedValue = taker.ExecutedValue.Add(executedValue)
		taker.FillFees = taker.FillFees.Add(askFee)

		balanceSub(m.baseCurrency, taker.UserId, ACCOUNT_TYPE_CASH, BALANCE_TYPE_AVAILABLE, amount)
		if real {
			appendBalanceTradeSub(taker, m.baseCurrency, amount, price, amount)
		}
		balanceAdd(m.quoteCurrency, taker.UserId, ACCOUNT_TYPE_CASH, BALANCE_TYPE_AVAILABLE, executedValue)
		if real {
			appendBalanceTradeAdd(taker, m.quoteCurrency, executedValue, price, amount)
		}

		if askFee.IsPositive() {
			balanceSub(m.quoteCurrency, taker.UserId, ACCOUNT_TYPE_CASH, BALANCE_TYPE_AVAILABLE, askFee)
			if real {
				appendBalanceTradeFee(taker, m.quoteCurrency, askFee, price, amount, taker.TakerFee)
			}
		}

		maker.RemainingAmount = maker.RemainingAmount.Sub(amount)
		maker.Freeze = maker.Freeze.Sub(executedValue)
		maker.FilledAmount = maker.FilledAmount.Add(amount)
		maker.ExecutedValue = maker.ExecutedValue.Add(executedValue)
		maker.FillFees = maker.FillFees.Add(bidFee)

		balanceSub(m.quoteCurrency, maker.UserId, ACCOUNT_TYPE_CASH, BALANCE_TYPE_HOLD, executedValue)
		if real {
			appendBalanceTradeSub(maker, m.quoteCurrency, executedValue, price, amount)
		}
		balanceAdd(m.baseCurrency, maker.UserId, ACCOUNT_TYPE_CASH, BALANCE_TYPE_AVAILABLE, amount)
		if real {
			appendBalanceTradeAdd(maker, m.baseCurrency, amount, price, amount)
		}

		if bidFee.IsPositive() {
			balanceSub(m.baseCurrency, maker.UserId, ACCOUNT_TYPE_CASH, BALANCE_TYPE_AVAILABLE, bidFee)
			if real {
				appendBalanceTradeFee(maker, m.baseCurrency, bidFee, price, amount, maker.MakerFee)
			}
		}

		if maker.RemainingAmount.IsZero() {
			if real {
				pushOrderMessage(ORDER_EVENT_TYPE_FINISH, maker, m.baseCurrency, m.quoteCurrency)
			}
			orderFinish(real, m, maker)
		} else {
			if real {
				pushOrderMessage(ORDER_EVENT_TYPE_UPDATE, maker, m.baseCurrency, m.quoteCurrency)
			}
		}
	}
}

func executeMarketBidOrder(real bool, m *currencyPair, taker *order) {
	var (
		price         decimal.Decimal
		amount        decimal.Decimal
		executedValue decimal.Decimal
		askFee        decimal.Decimal
		bidFee        decimal.Decimal
		result        decimal.Decimal
	)

	for e := m.asks.Front(); e != nil; e = e.Next() {
		if taker.RemainingAmount.IsZero() {
			break
		}

		maker := e.Value().(*order)
		price = maker.Price
		// TODO: check this, maybe DivRound
		amount = taker.RemainingAmount.Div(price)
		for {
			result = amount.Mul(price)
			if result.GreaterThan(taker.RemainingAmount) {
				// TODO
				result, _ = decimal.NewFromString("0.00000001")
				amount = amount.Sub(result)
			} else {
				break
			}
		}

		if amount.GreaterThan(maker.RemainingAmount) {
			amount = maker.RemainingAmount
		}
		if amount.IsZero() {
			break
		}

		executedValue = price.Mul(amount)
		askFee = executedValue.Mul(maker.MakerFee)
		bidFee = amount.Mul(taker.TakerFee)

		now := time.Now()
		taker.UpdatedAt = now
		maker.UpdatedAt = now
		id, err := uuid.NewV4()
		if err != nil {
			log.Error().Err(err).Msg("failed to generate UUID")
			break
		}
		dealId := id.String()
		if real {
			appendOrderDealHistory(utils.UnixMilli(taker.UpdatedAt), dealId, maker, MARKET_ROLE_MAKER, taker, MARKET_ROLE_TAKER, price, amount, executedValue, askFee, bidFee)
			pushDealMessage(utils.UnixMilli(taker.UpdatedAt), m.id, maker, taker, price, amount, askFee, bidFee, MARKET_ORDER_SIDE_BID, dealId, m.baseCurrency, m.quoteCurrency)
		}

		taker.RemainingAmount = taker.RemainingAmount.Sub(amount) // TODO: should be amount?
		taker.FilledAmount = taker.FilledAmount.Add(amount)
		taker.ExecutedValue = taker.ExecutedValue.Add(executedValue)
		taker.FillFees = taker.FillFees.Add(bidFee)

		balanceSub(m.quoteCurrency, taker.UserId, ACCOUNT_TYPE_CASH, BALANCE_TYPE_AVAILABLE, executedValue)
		if real {
			appendBalanceTradeSub(taker, m.quoteCurrency, executedValue, price, amount)
		}
		balanceAdd(m.baseCurrency, taker.UserId, ACCOUNT_TYPE_CASH, BALANCE_TYPE_AVAILABLE, amount)
		if real {
			appendBalanceTradeAdd(taker, m.baseCurrency, amount, price, amount)
		}

		if bidFee.IsPositive() {
			balanceSub(m.baseCurrency, taker.UserId, ACCOUNT_TYPE_CASH, BALANCE_TYPE_AVAILABLE, bidFee)
			if real {
				appendBalanceTradeFee(taker, m.baseCurrency, bidFee, price, amount, taker.TakerFee)
			}
		}

		maker.RemainingAmount = maker.RemainingAmount.Sub(amount)
		maker.Freeze = maker.Freeze.Sub(amount)
		maker.FilledAmount = maker.FilledAmount.Add(amount)
		maker.ExecutedValue = maker.ExecutedValue.Add(executedValue)
		maker.FillFees = maker.FillFees.Add(askFee)

		balanceSub(m.baseCurrency, maker.UserId, ACCOUNT_TYPE_CASH, BALANCE_TYPE_HOLD, amount)
		if real {
			appendBalanceTradeSub(maker, m.baseCurrency, amount, price, amount)
		}
		balanceAdd(m.quoteCurrency, maker.UserId, ACCOUNT_TYPE_CASH, BALANCE_TYPE_AVAILABLE, executedValue)
		if real {
			appendBalanceTradeAdd(maker, m.quoteCurrency, executedValue, price, amount)
		}

		if askFee.IsPositive() {
			balanceSub(m.quoteCurrency, maker.UserId, ACCOUNT_TYPE_CASH, BALANCE_TYPE_AVAILABLE, askFee)
			if real {
				appendBalanceTradeFee(maker, m.quoteCurrency, askFee, price, amount, maker.MakerFee)
			}
		}

		if maker.RemainingAmount.IsZero() {
			if real {
				pushOrderMessage(ORDER_EVENT_TYPE_FINISH, maker, m.baseCurrency, m.quoteCurrency)
			}
			orderFinish(real, m, maker)
		} else {
			if real {
				pushOrderMessage(ORDER_EVENT_TYPE_UPDATE, maker, m.baseCurrency, m.quoteCurrency)
			}
		}
	}
}

func marketPutMarketOrder(real bool, m *currencyPair, userId string, side string, _amount string) *order {
	amount, err := decimal.NewFromString(_amount)
	if err != nil {
		log.Error().Str("amount", _amount).Msg("wrong decimal")
		return nil
	}
	if side == "sell" {
		balance := balanceGet(m.baseCurrency, userId, ACCOUNT_TYPE_CASH).available
		if balance.LessThan(amount) {
			// TODO: handle error
			return nil
		}
		e := m.bids.Front()
		if e == nil {
			return nil
		}
		// if amount < m.fMinAmount {
		// 	return ""
		// }
	} else {
		balance := balanceGet(m.quoteCurrency, userId, ACCOUNT_TYPE_CASH).available
		if balance.LessThan(amount) {
			return nil
		}
		e := m.asks.Front()
		if e == nil {
			return nil
		}
		o := e.Value().(*order)
		// TODO
		require := o.Price.Mul(decimal.NewFromFloat(0.001))
		if amount.LessThan(require) {
			return nil
		}
	}

	now := time.Now()
	o := new(order)
	o.UserId = userId
	id, err := uuid.NewV4()
	if err != nil {
		log.Error().Err(err).Msg("failed to generate UUID")
		return nil
	}
	o.Id = id.String()
	o.Price = decimal.Zero
	o.Amount = amount
	o.Symbol = m.id
	o.Side = side
	o.Type = "market"
	o.CreatedAt = now
	o.UpdatedAt = now
	o.FillFees = decimal.Zero
	o.FilledAmount = decimal.Zero
	o.RemainingAmount = amount
	o.ExecutedValue = decimal.Zero

	o.TakerFee = m.takerFee
	o.MakerFee = decimal.Zero // TODO

	o.Freeze = decimal.Zero

	if side == "sell" {
		executeMarketAskOrder(real, m, o)
	} else { // "buy"
		executeMarketBidOrder(real, m, o)
	}
	// TODO: handle error
	if real {
		appendOrderHistory(o)
		pushOrderMessage(ORDER_EVENT_TYPE_FINISH, o, m.baseCurrency, m.quoteCurrency)
	}
	return o
}

func marketCancelOrder(real bool, m *currencyPair, o *order) *order {
	if real {
		pushOrderMessage(ORDER_EVENT_TYPE_FINISH, o, m.baseCurrency, m.quoteCurrency)
	}
	orderFinish(real, m, o)
	// TODO: remove return value?
	return o
}

func marketGetStatus(m *currencyPair) (int64, string, int64, string) {
	askCount := int64(m.asks.Length)
	bidCount := int64(m.bids.Length)
	askAmount := decimal.Zero
	bidAmount := decimal.Zero
	for e := m.asks.Front(); e != nil; e = e.Next() {
		o := e.Value().(*order)
		askAmount = askAmount.Add(o.RemainingAmount)
	}

	for e := m.bids.Front(); e != nil; e = e.Next() {
		o := e.Value().(*order)
		bidAmount = bidAmount.Add(o.RemainingAmount)
	}
	return askCount, askAmount.String(), bidCount, bidAmount.String()
}

func orderPut(real bool, m *currencyPair, o *order) {
	if o.Type != "limit" {
		// TODO: log error
		log.Error().Str("type", o.Type).Msg("order type error")
		return
	}

	// TODO: check dup key
	m.orders[o.Id] = o

	_, ok := m.ordersByUser[o.UserId]
	if !ok {
		m.ordersByUser[o.UserId] = list.New()
	}
	m.ordersByUser[o.UserId].PushFront(o.Id)

	if o.Side == "sell" {
		m.asks.Set(o.Price, o)
		o.Freeze = o.RemainingAmount
		// TODO: handle error
		balanceFreeze(m.baseCurrency, o.UserId, ACCOUNT_TYPE_CASH, o.RemainingAmount)
	} else {
		m.bids.Set(o.Price, o)
		result := o.Price.Mul(o.RemainingAmount)
		o.Freeze = result
		balanceFreeze(m.quoteCurrency, o.UserId, ACCOUNT_TYPE_CASH, result)
	}
	b, err := json.Marshal(o)
	if err != nil {
		log.Error().Err(err).Msg("failed to marshal kafka order message")
		return
	}
	if real {
		producer.ProduceChannel() <- &kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &topicSirenOrders, Partition: kafka.PartitionAny}, Value: b, Key: []byte(o.Id)}
	}
}

// TODO: send balance to kafka?
func orderFinish(real bool, m *currencyPair, o *order) {
	if o.Side == "sell" {
		e := m.asks.Remove(o.Price)
		// check
		node := e.Value().(*order)
		if node != o {
			log.Print("order not match")
		}
		if o.Freeze.IsPositive() {
			balanceUnfreeze(m.baseCurrency, o.UserId, ACCOUNT_TYPE_CASH, o.Freeze)
		}
	} else {
		e := m.bids.Remove(o.Price)
		// check
		node := e.Value().(*order)
		if node != o {
			log.Print("order not match")
		}
		if o.Freeze.IsPositive() {
			balanceUnfreeze(m.quoteCurrency, o.UserId, ACCOUNT_TYPE_CASH, o.Freeze)
		}
	}

	delete(m.orders, o.Id)
	if orders, ok := m.ordersByUser[o.UserId]; ok {
		for e := orders.Front(); e != nil; e = e.Next() {
			if e.Value == o.Id {
				orders.Remove(e)
				log.Info().Str("order_id", o.Id).Msg("order finished, remove")
				break
			}
		}
	}

	if real {
		if o.FilledAmount.IsPositive() {
			appendOrderHistory(o)
			// TODO: handle error
		}
	}
}
