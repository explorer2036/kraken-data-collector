package kraken

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"github.com/sirupsen/logrus"
)

const (
	krakenWSURL = "wss://ws.kraken.com"

	krakenWsHeartbeat          = "heartbeat"
	krakenWsPong               = "pong"
	KrankenWsPing              = "ping"
	krakenWsSystemStatus       = "systemStatus"
	krakenWsSubscribe          = "subscribe"
	krakenWsSubscriptionStatus = "subscriptionStatus"

	krakenWsOrderbook            = "book"
	krakenWsOrderbookAskSnapshot = "as"
	krakenWsOrderbookBidSnapshot = "bs"
	krakenWsOrderbookAsk         = "a"
	krakenWsOrderbookBid         = "b"
	krakenWsOrderbookCheckum     = "c"
	krakenWsOrderbookDepth       = 1000

	// online|maintenance|cancel_only|limit_only|post_only
	krakenSystemStatusOnline = "online"

	krakenSubscriptionStatusSubscribed   = "subscribed"
	krakenSubscriptionStatusUnSubscirbed = "unsubscribed"
	krakenSubscriptionStatusError        = "error"

	krakenWsPingDelay = 30 // 30 s

	krakenChecksumCalculation = 10
)

var (
	errClosedConnection = errors.New("use of closed network connection")
)

// initStream create a websocket connection
func (s *Kraken) initStream(ctx context.Context) error {
	conn, _, err := websocket.DefaultDialer.DialContext(ctx, krakenWSURL, http.Header{})
	if err != nil {
		return fmt.Errorf("ws dial: %w", err)
	}
	s.conn = conn

	// receive the message by websocket
	go s.receiveMessage(ctx)

	// ping server to determine whether connection is alive, server responds with pong
	go s.setupPingHandler(ctx)

	return nil
}

// isDisconnectionError Determines if the error sent over chan ReadMessageErrors is a disconnection error
func isDisconnectionError(err error) bool {
	if websocket.IsUnexpectedCloseError(err) {
		return true
	}
	if _, ok := err.(*net.OpError); ok {
		return !errors.Is(err, errClosedConnection)
	}
	return false
}

func (s *Kraken) receiveMessage(ctx context.Context) {
	for {
		_, data, err := s.conn.ReadMessage()
		if err != nil {
			select {
			case <-ctx.Done():
				return
			default:
			}
			if isDisconnectionError(err) {
				return
			}
			// rebuild the websocket connection
			logrus.Errorf("read message: %v", err)
			return
		}
		if err := s.handleMessage(data); err != nil {
			logrus.Errorf("%v", err)
		}
	}
}

func sortOrderBook(items map[string]*OrderBookItem, ascending bool) []*OrderBookItem {
	keys := make([]string, 0, len(items))
	for key := range items {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	if !ascending {
		sort.Sort(sort.Reverse(sort.StringSlice(keys)))
	}
	sortedItems := make([]*OrderBookItem, 0, len(keys))
	for _, key := range keys {
		sortedItems = append(sortedItems, items[key])
	}
	return sortedItems
}

func (s *Kraken) validateCRC32(asks, bids []*OrderBookItem, token string) bool {
	var tokenBuilder strings.Builder
	for i := 0; i < 10; i++ {
		price := strings.Replace(asks[i].Price, ".", "", 1)
		price = strings.TrimLeft(price, "0")
		tokenBuilder.WriteString(price)

		quantity := strings.Replace(asks[i].Quantity, ".", "", 1)
		quantity = strings.TrimLeft(quantity, "0")
		tokenBuilder.WriteString(quantity)
	}

	for i := 0; i < 10; i++ {
		price := strings.Replace(bids[i].Price, ".", "", 1)
		price = strings.TrimLeft(price, "0")
		tokenBuilder.WriteString(price)

		quantity := strings.Replace(bids[i].Quantity, ".", "", 1)
		quantity = strings.TrimLeft(quantity, "0")
		tokenBuilder.WriteString(quantity)
	}
	number, _ := strconv.ParseUint(token, 10, 32)

	return crc32.ChecksumIEEE([]byte(tokenBuilder.String())) == uint32(number)
}

func (s *Kraken) handleOrderBook(pair string, update *OrderBookUpdate) error {
	s.obsMutex.Lock()
	defer s.obsMutex.Unlock()

	ob, ok := s.obs[pair]
	if !ok {
		ob = &CachedOrderBook{
			Asks: make(map[string]*OrderBookItem),
			Bids: make(map[string]*OrderBookItem),
		}
		s.obs[pair] = ob
	}
	var lastUpdatedTime string
	// Ask data is not always sent
	for _, ask := range update.Asks {
		key := ask.Price
		if ask.Deleted {
			delete(ob.Asks, key)
			continue
		}

		item, ok := ob.Asks[key]
		if !ok {
			item = &OrderBookItem{
				Price:    ask.Price,
				Quantity: ask.Quantity,
			}
		} else {
			item.Quantity = ask.Quantity
		}
		ob.Asks[key] = item

		if lastUpdatedTime < ask.Timestamp {
			lastUpdatedTime = ask.Timestamp
		}
	}

	// Bid data is not always sent
	for _, bid := range update.Bids {
		key := bid.Price
		if bid.Deleted {
			delete(ob.Bids, key)
			continue
		}

		item, ok := ob.Bids[key]
		if !ok {
			item = &OrderBookItem{
				Price:    bid.Price,
				Quantity: bid.Quantity,
			}
		} else {
			item.Quantity = bid.Quantity
		}
		ob.Bids[key] = item

		if lastUpdatedTime < bid.Timestamp {
			lastUpdatedTime = bid.Timestamp
		}
	}
	ob.LastUpdated = lastUpdatedTime

	// no need checksum for snapshot
	if update.Snapshot {
		return nil
	}

	// checksum is based on top 10 asks and bids
	if len(ob.Asks) < krakenChecksumCalculation || len(ob.Bids) < krakenChecksumCalculation {
		return nil
	}

	// the top ten ask price levels should be sorted by price from low to high.
	// the top ten bid price levels should be sorted by price from high to low.
	asks := sortOrderBook(ob.Asks, true)
	bids := sortOrderBook(ob.Bids, false)

	// validate the checksum of the update
	if !s.validateCRC32(asks[:krakenChecksumCalculation], bids[:krakenChecksumCalculation], update.CheckSum) {
		ob.Failure += 1
	} else {
		ob.Success += 1
	}

	return nil
}

func (s *Kraken) handlePublicMessage(data []byte) error {
	var message KrakenMessage
	if err := json.Unmarshal(data, &message); err != nil {
		return err
	}

	channel := strings.Split(message.ChannelName, "-")[0]
	switch channel {
	case krakenWsOrderbook:
		var orderbook OrderBookUpdate
		if err := json.Unmarshal(message.Data, &orderbook); err != nil {
			return err
		}
		if err := s.handleOrderBook(message.Pair, &orderbook); err != nil {
			return fmt.Errorf("handle order book: %w", err)
		}
	default:
		return fmt.Errorf("received unsubscribed data: %s", data)
	}
	return nil
}

func (s *Kraken) handleGeneralMessage(data []byte) error {
	var fields map[string]interface{}
	if err := json.Unmarshal(data, &fields); err != nil {
		return fmt.Errorf("json unmarshal: %w", err)
	}
	event, ok := fields["event"]
	if !ok {
		return fmt.Errorf("received unknown message: %s", data)
	}

	switch event {
	case krakenWsPong, krakenWsHeartbeat:
	case krakenWsSystemStatus:
		var status SystemStatus
		if err := json.Unmarshal(data, &status); err != nil {
			return fmt.Errorf("json unmarshal: %w", err)
		}
		if status.Status != krakenSystemStatusOnline {
			logrus.Warnf("kraken system status: %s", status.Status)
		}
	case krakenWsSubscriptionStatus:
		var status SubscriptionStatus
		if err := json.Unmarshal(data, &status); err != nil {
			return fmt.Errorf("json unmarshal: %w", err)
		}
		if status.Status == krakenSubscriptionStatusError {
			return fmt.Errorf("subscribe %s: %s", status.ChannelName, status.ErrorMessage)
		}
	default:
		return fmt.Errorf("event %s is not supported: %s", event, data)
	}
	return nil
}

func (s *Kraken) handleMessage(data []byte) error {
	if len(data) == 0 {
		return errors.New("received empty message")
	}
	switch data[0] {
	case '[': // handle the public message
		return s.handlePublicMessage(data)
	case '{': // handle the general message
		return s.handleGeneralMessage(data)
	default:
		return fmt.Errorf("unexpected message: %s", data)
	}
}

func (s *Kraken) setupPingHandler(ctx context.Context) error {
	data, err := json.Marshal(PingRequest{Event: KrankenWsPing})
	if err != nil {
		return err
	}
	go func() {
		ticker := time.NewTicker(time.Second * krakenWsPingDelay)
		for {
			select {
			case <-ctx.Done():
				ticker.Stop()
				return
			case <-ticker.C:
				if err := s.conn.WriteMessage(websocket.TextMessage, data); err != nil {
					logrus.Errorf("write ping message: %v", err)
				}
			}
		}
	}()
	return nil
}

func (s *Kraken) doSubscribe(name string, pairs []string) error {
	subscription := &SubscriptionEvent{
		Event: krakenWsSubscribe,
		Pair:  pairs,
		Subscription: &Subscription{
			Name: name,
		},
	}
	return s.conn.WriteJSON(subscription)
}
