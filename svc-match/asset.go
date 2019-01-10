package main

import (
	"errors"
	"sync"

	"github.com/en/siren/utils"
	"github.com/shopspring/decimal"

	log "github.com/en/siren/utils/glog"
)

type currency struct {
	id      string `json:"id"`
	name    string `json:"name"`
	minSize string `json:"min_size"`
}

type AccountType int32

const (
	ACCOUNT_TYPE_CASH   AccountType = 0
	ACCOUNT_TYPE_MARGIN AccountType = 1
)

type BalanceType int32

const (
	BALANCE_TYPE_AVAILABLE BalanceType = 1
	BALANCE_TYPE_HOLD      BalanceType = 2
)

type account struct {
	available decimal.Decimal
	hold      decimal.Decimal
}

type user struct {
	sync.RWMutex

	cash   map[string]account
	margin map[string]account
}

type asset struct {
	sync.RWMutex

	users map[string]*user
}

var assets asset
var currencies map[string]currency

func balanceGet(currencyId string, userId string, accountType AccountType) account {
	assets.RLock()
	u, ok := assets.users[userId]
	assets.RUnlock()
	if !ok {
		return account{}
	}

	u.RLock()
	defer u.RUnlock()
	if accountType == ACCOUNT_TYPE_CASH {
		return u.cash[currencyId]
	} else {
		return u.margin[currencyId]
	}
}

func balanceSet(currencyId string, userId string, accountType AccountType, available string, hold string) error {
	_, ok := currencies[currencyId]
	if !ok {
		return errors.New("currency not found")
	}

	assets.RLock()
	u, ok := assets.users[userId]
	assets.RUnlock()

	a, err := decimal.NewFromString(available)
	if err != nil {
		log.Error().Str("available", available).Msg("wrong decimal")
		return nil
	}
	h, err := decimal.NewFromString(hold)
	if err != nil {
		log.Error().Str("hold", hold).Msg("wrong decimal")
		return nil
	}
	if ok {
		u.Lock()
		if accountType == ACCOUNT_TYPE_CASH {
			u.cash[currencyId] = account{available: a, hold: h}
		} else {
			u.margin[currencyId] = account{available: a, hold: h}
		}
		u.Unlock()
	} else {
		u := new(user)
		u.cash = make(map[string]account)
		u.margin = make(map[string]account)
		if accountType == ACCOUNT_TYPE_CASH {
			u.cash[currencyId] = account{available: a, hold: h}
		} else {
			u.margin[currencyId] = account{available: a, hold: h}
		}
		assets.Lock()
		assets.users[userId] = u
		assets.Unlock()
	}

	return nil
}

func balanceAdd(currencyId string, userId string, accountType AccountType, balanceType BalanceType, amount decimal.Decimal) error {
	_, ok := currencies[currencyId]
	if !ok {
		return errors.New("currency not found")
	}

	assets.RLock()
	u, ok := assets.users[userId]
	assets.RUnlock()
	if ok {
		u.Lock()
		if accountType == ACCOUNT_TYPE_CASH {
			old := u.cash[currencyId]
			if balanceType == BALANCE_TYPE_AVAILABLE {
				u.cash[currencyId] = account{available: old.available.Add(amount), hold: old.hold}
			} else {
				u.cash[currencyId] = account{available: old.available, hold: old.hold.Add(amount)}
			}
		} else {
			old := u.margin[currencyId]
			if balanceType == BALANCE_TYPE_AVAILABLE {
				u.margin[currencyId] = account{available: old.available.Add(amount), hold: old.hold}
			} else {
				u.margin[currencyId] = account{available: old.available, hold: old.hold.Add(amount)}
			}
		}
		u.Unlock()
	} else {
		u := new(user)
		u.cash = make(map[string]account)
		u.margin = make(map[string]account)
		if accountType == ACCOUNT_TYPE_CASH {
			if balanceType == BALANCE_TYPE_AVAILABLE {
				u.cash[currencyId] = account{available: amount, hold: decimal.Zero}
			} else {
				u.cash[currencyId] = account{available: decimal.Zero, hold: amount}
			}
		} else {
			if balanceType == BALANCE_TYPE_AVAILABLE {
				u.margin[currencyId] = account{available: amount, hold: decimal.Zero}
			} else {
				u.margin[currencyId] = account{available: decimal.Zero, hold: amount}
			}
		}
		assets.Lock()
		assets.users[userId] = u
		assets.Unlock()
	}

	return nil
}

func balanceSub(currencyId string, userId string, accountType AccountType, balanceType BalanceType, amount decimal.Decimal) error {
	_, ok := currencies[currencyId]
	if !ok {
		return errors.New("currency not found")
	}

	assets.RLock()
	u, ok := assets.users[userId]
	assets.RUnlock()

	if !ok {
		return errors.New("user_id not found")
	}

	u.Lock()
	defer u.Unlock()
	if accountType == ACCOUNT_TYPE_CASH {
		old := u.cash[currencyId]
		if balanceType == BALANCE_TYPE_AVAILABLE {
			if old.available.LessThan(amount) {
				return errors.New("not enough amount to sub")
			}
			u.cash[currencyId] = account{available: old.available.Sub(amount), hold: old.hold}
		} else {
			if old.hold.LessThan(amount) {
				return errors.New("not enough amount to sub")
			}
			u.cash[currencyId] = account{available: old.available, hold: old.hold.Sub(amount)}
		}
	} else {
		old := u.margin[currencyId]
		if balanceType == BALANCE_TYPE_AVAILABLE {
			if old.available.LessThan(amount) {
				return errors.New("not enough amount to sub")
			}
			u.margin[currencyId] = account{available: old.available.Sub(amount), hold: old.hold}
		} else {
			if old.hold.LessThan(amount) {
				return errors.New("not enough amount to sub")
			}
			u.margin[currencyId] = account{available: old.available, hold: old.hold.Sub(amount)}
		}
	}

	return nil
}

func balanceFreeze(currencyId string, userId string, accountType AccountType, amount decimal.Decimal) error {
	_, ok := currencies[currencyId]
	if !ok {
		return errors.New("currency not found")
	}

	assets.RLock()
	u, ok := assets.users[userId]
	assets.RUnlock()

	if !ok {
		return errors.New("user_id not found")
	}

	u.Lock()
	defer u.Unlock()
	if accountType == ACCOUNT_TYPE_CASH {
		old := u.cash[currencyId]
		if old.available.LessThan(amount) {
			return errors.New("not enough amount to freeze")
		}
		u.cash[currencyId] = account{available: old.available.Sub(amount), hold: old.hold.Add(amount)}
	} else {
		old := u.margin[currencyId]
		if old.available.LessThan(amount) {
			return errors.New("not enough amount to freeze")
		}
		u.margin[currencyId] = account{available: old.available.Sub(amount), hold: old.hold.Add(amount)}
	}

	return nil
}

func balanceUnfreeze(currencyId string, userId string, accountType AccountType, amount decimal.Decimal) error {
	_, ok := currencies[currencyId]
	if !ok {
		return errors.New("currency not found")
	}

	assets.RLock()
	u, ok := assets.users[userId]
	assets.RUnlock()

	if !ok {
		return errors.New("user_id not found")
	}

	u.Lock()
	defer u.Unlock()
	if accountType == ACCOUNT_TYPE_CASH {
		old := u.cash[currencyId]
		if old.hold.LessThan(amount) {
			return errors.New("not enough amount to freeze")
		}
		u.cash[currencyId] = account{available: old.available.Add(amount), hold: old.hold.Sub(amount)}
	} else {
		old := u.margin[currencyId]
		if old.hold.LessThan(amount) {
			return errors.New("not enough amount to freeze")
		}
		u.margin[currencyId] = account{available: old.available.Add(amount), hold: old.hold.Sub(amount)}
	}

	return nil
}

type status struct {
	total          decimal.Decimal
	available      decimal.Decimal
	hold           decimal.Decimal
	availableCount int64
	holdCount      int64
}

func balanceStatus(currencyId string, accountType AccountType) status {
	var s status

	_, ok := currencies[currencyId]
	if !ok {
		return s
	}

	assets.RLock()
	for _, u := range assets.users {
		u.RLock()
		if accountType == ACCOUNT_TYPE_CASH {
			balance := u.cash[currencyId]
			if balance.available.IsPositive() {
				s.total = s.total.Add(balance.available)
				s.available = s.available.Add(balance.available)
				s.availableCount += 1
			}

			if balance.hold.IsPositive() {
				s.total = s.total.Add(balance.hold)
				s.hold = s.hold.Add(balance.hold)
				s.holdCount += 1
			}
		} else {
			balance := u.margin[currencyId]
			if balance.available.IsPositive() {
				s.total = s.total.Add(balance.available)
				s.available = s.available.Add(balance.available)
				s.availableCount += 1
			}

			if balance.hold.IsPositive() {
				s.total = s.total.Add(balance.hold)
				s.hold = s.hold.Add(balance.hold)
				s.holdCount += 1
			}
		}
		u.RUnlock()
	}
	assets.RUnlock()
	return s
}

func updateUserBalance(push bool, id string, typ string, userId string, currencyId string, accountType AccountType, amount decimal.Decimal) {
	_, ok := currencies[currencyId]
	if !ok {
		log.Error().Str("currency_id", currencyId).Msg("currency_id not exists")
		return
	}

	assets.RLock()
	_, ok = assets.users[userId]
	assets.RUnlock()
	if !ok {
		log.Info().Str("user_id", userId).Msg("user not found, create one")
		newUser := new(user)
		newUser.cash = make(map[string]account)
		newUser.margin = make(map[string]account)
		assets.Lock()
		assets.users[userId] = newUser
		assets.Unlock()
	}

	var err error
	if typ == "Deposit" {
		err = balanceAdd(currencyId, userId, ACCOUNT_TYPE_CASH, BALANCE_TYPE_AVAILABLE, amount)
	} else {
		err = balanceSub(currencyId, userId, ACCOUNT_TYPE_CASH, BALANCE_TYPE_AVAILABLE, amount)
	}

	if err != nil {
		log.Error().Err(err).Msg("failed to update balance")
		return
	}
	if push {
		now := utils.NowUnixMilli()
		if typ == "Deposit" {
			appendUserBalanceHistory(now, userId, currencyId, typ, amount, id)
		} else {
			appendUserBalanceHistory(now, userId, currencyId, typ, amount.Neg(), id)
		}
		pushBalanceMessage(now, userId, currencyId, typ, amount)
	}
}
