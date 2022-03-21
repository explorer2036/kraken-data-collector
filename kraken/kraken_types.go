package kraken

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// KrakenMessage - data structure of default Kraken WS update
type KrakenMessage struct {
	ChannelID   int64
	Data        json.RawMessage
	ChannelName string
	Pair        string
}

func (s *KrakenMessage) UnmarshalJSON(data []byte) error {
	var fields []json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	if len(fields) < 4 {
		return fmt.Errorf("invalid message: %s", data)
	}
	if len(fields) == 5 {
		asks, bids := fields[1], fields[2]
		if (asks[0] != '{' && asks[len(asks)-1] != '}') || (bids[0] != '{' && bids[len(bids)-1] != '}') {
			return fmt.Errorf("invalid message: %s", data)
		}
		// merge asks and bids
		merged := make([]byte, 0, len(asks)+len(bids)-1)
		merged = append(merged, asks[0:len(asks)-1]...)
		merged = append(merged, ',')
		merged = append(merged, bids[1:]...)
		encoded, err := json.Marshal([]json.RawMessage{fields[0], merged, fields[3], fields[4]})
		if err != nil {
			return err
		}
		data = encoded
	}
	body := []interface{}{
		&s.ChannelID,
		&s.Data,
		&s.ChannelName,
		&s.Pair,
	}
	return json.Unmarshal(data, &body)
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

type OrderBookItem struct {
	Price     string
	Quantity  string
	Timestamp string
	Republish bool
	Deleted   bool
}

func (s *OrderBookItem) UnmarshalJSON(data []byte) error {
	var fields []json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	if len(fields) < 3 {
		return fmt.Errorf("invalid order book item: %s", data)
	}
	s.Republish = len(fields) == 4

	if err := json.Unmarshal(fields[0], &s.Price); err != nil {
		return err
	}
	if err := json.Unmarshal(fields[1], &s.Quantity); err != nil {
		return err
	}
	if err := json.Unmarshal(fields[2], &s.Timestamp); err != nil {
		return err
	}
	s.Timestamp = strings.Split(s.Timestamp, ".")[0]

	// if the quantity is zero, delete the price from snapshot
	quantity, err := strconv.ParseFloat(s.Quantity, 64)
	if err != nil {
		return err
	}
	if quantity == 0 {
		s.Deleted = true
	}

	return nil
}

type CachedOrderBook struct {
	Asks        map[string]*OrderBookItem
	Bids        map[string]*OrderBookItem
	LastUpdated string
	Failure     uint64 // checksum failed
	Success     uint64 // checksum success
}

type SubscriptionChannel struct {
	Subscription string
	Pair         string
	ChannelName  string
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

type OrderBookSnapshot struct {
	Pair      string        `json:"-"`
	Timestamp string        `json:"timestamp"`
	Body      OrderBookBody `json:"snapshot"`
}

type OrderBookBody struct {
	Asks []interface{} `json:"asks"`
	Bids []interface{} `json:"bids"`
}
