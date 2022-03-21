package kraken

import (
	"encoding/json"
	"fmt"
	"time"
)

// KrakenMessage - data structure of default Kraken WS update
type KrakenMessage struct {
	ChannelID   int64
	Data        json.RawMessage
	ChannelName string
	Pair        string
}

// OrderBookUpdate - data structure for order book update
type OrderBookUpdate struct {
	Asks     []OrderBookItem
	Bids     []OrderBookItem
	CheckSum string
	Snapshot bool
}

func (s *OrderBookUpdate) UnmarshalJSON(data []byte) error {
	fields := make(map[string]json.RawMessage)
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	for key, value := range fields {
		if len(key) == 2 {
			s.Snapshot = true
		}
		switch key[0] {
		case 'a':
			if err := json.Unmarshal(value, &s.Asks); err != nil {
				return err
			}
		case 'b':
			if err := json.Unmarshal(value, &s.Bids); err != nil {
				return err
			}
		case 'c':
			if err := json.Unmarshal(value, &s.CheckSum); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unexpected key: %s", key)
		}
	}
	return nil
}

type SubscriptionChannel struct {
	Subscription string
	Pair         string
	ChannelName  string
}

type OrderBook struct {
	Asks        map[string]*OrderBookItem
	Bids        map[string]*OrderBookItem
	LastUpdated time.Time
}

type OrderBookItem struct {
	Price     string
	Quantity  string
	Time      json.Number
	Republish bool
}

type SystemStatus struct {
	ConnectionID float64 `json:"connectionID"`
	Event        string  `json:"event"`
	Status       string  `json:"status"`
	Version      string  `json:"version"`
}

type SubscriptionStatus struct {
	ChannelName  string `json:"channelName"`
	ErrorMessage string `json:"errorMessage"`
	Event        string `json:"event"`
	Pair         string `json:"pair"`
	Status       string `json:"status"`
	Subscription struct {
		Name string `json:"name"`
	} `json:"subscription"`
}

type Subscription struct {
	Name  string `json:"name"`
	Depth int    `json:"depth,omitempty"`
}

type SubscriptionEvent struct {
	Event        string        `json:"event"`
	Pair         []string      `json:"pair"`
	Subscription *Subscription `json:"subscription"`
}

type PingRequest struct {
	Event string `json:"event"`
}

// AssetPairs holds asset pair information
type AssetPairs struct {
	Altname           string      `json:"altname"`
	Wsname            string      `json:"wsname"`
	AclassBase        string      `json:"aclass_base"`
	Base              string      `json:"base"`
	AclassQuote       string      `json:"aclass_quote"`
	Quote             string      `json:"quote"`
	Lot               string      `json:"lot"`
	PairDecimals      int         `json:"pair_decimals"`
	LotDecimals       int         `json:"lot_decimals"`
	LotMultiplier     int         `json:"lot_multiplier"`
	LeverageBuy       []int       `json:"leverage_buy"`
	LeverageSell      []int       `json:"leverage_sell"`
	Fees              [][]float64 `json:"fees"`
	FeesMaker         [][]float64 `json:"fees_maker"`
	FeeVolumeCurrency string      `json:"fee_volume_currency"`
	MarginCall        int         `json:"margin_call"`
	MarginStop        int         `json:"margin_stop"`
	Ordermin          string      `json:"ordermin"`
}
