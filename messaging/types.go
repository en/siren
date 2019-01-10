package messaging

import (
	"time"

	"github.com/shopspring/decimal"
)

type TransferMessage struct {
	Id        string          `json:"id"`
	UserId    string          `json:"user_id"`
	Currency  string          `json:"currency"`
	Type      string          `json:"type"`
	Amount    decimal.Decimal `json:"amount"`
	Status    string          `json:"status"`
	CreatedAt time.Time       `json:"created_at"`
}
