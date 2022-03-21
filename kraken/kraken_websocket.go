package kraken

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
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
			logrus.Errorf("handle message: %v", err)
		}
	}
}

func (s *Kraken) addSubscriptionChannel(status *SubscriptionStatus) {
	s.Lock()
	defer s.Unlock()
	s.subs[status.Pair] = &SubscriptionChannel{
		Subscription: status.Subscription.Name,
		Pair:         status.Pair,
		ChannelName:  status.ChannelName,
	}
}

func (s *Kraken) findSubscriptionChannel(key string) *SubscriptionChannel {
	s.Lock()
	defer s.Unlock()
	return s.subs[key]
}

// timeFromUnixTimestampDecimal converts a unix timestamp in decimal form to
// a time.Time
func timeFromUnixTimestampDecimal(input float64) time.Time {
	i, f := math.Modf(input)
	return time.Unix(int64(i), int64(f*(1e9)))
}

func (s *Kraken) handleOrderBookSnapshot(sc *SubscriptionChannel, as, bs []interface{}) error {
	s.obsMutex.Lock()
	defer s.obsMutex.Unlock()

	ob, ok := s.obs[sc.Pair]
	if !ok {
		ob = &OrderBook{
			Asks: make(map[string]*OrderBookItem),
			Bids: make(map[string]*OrderBookItem),
		}
	}

	var lastUpdatedTime time.Time
	for i := range as {
		asks, ok := as[i].([]interface{})
		if !ok {
			return errors.New("type asset asks")
		}
		if len(asks) < 3 {
			return errors.New("invalid asks len")
		}
		price, quantity := asks[0].(string), asks[1].(string)

		value, err := strconv.ParseFloat(quantity, 64)
		if err != nil {
			return fmt.Errorf("parse quantity: %w", err)
		}
		if value == 0 {
			continue
		}
		ob.Asks[price] = &OrderBookItem{
			Price:    price,
			Quantity: quantity,
		}

		timestamp, err := strconv.ParseFloat(asks[2].(string), 64)
		if err != nil {
			return err
		}
		askUpdatedTime := timeFromUnixTimestampDecimal(timestamp)
		if lastUpdatedTime.Before(askUpdatedTime) {
			lastUpdatedTime = askUpdatedTime
		}
	}

	for i := range bs {
		bids, ok := bs[i].([]interface{})
		if !ok {
			return errors.New("type asset bids")
		}
		if len(bids) < 3 {
			return errors.New("invalid bids len")
		}
		price, quantity := bids[0].(string), bids[1].(string)

		value, err := strconv.ParseFloat(quantity, 64)
		if err != nil {
			return fmt.Errorf("parse quantity: %w", err)
		}
		if value == 0 {
			continue
		}
		ob.Bids[price] = &OrderBookItem{
			Price:    price,
			Quantity: quantity,
		}

		timestamp, err := strconv.ParseFloat(bids[2].(string), 64)
		if err != nil {
			return err
		}
		askUpdatedTime := timeFromUnixTimestampDecimal(timestamp)
		if lastUpdatedTime.Before(askUpdatedTime) {
			lastUpdatedTime = askUpdatedTime
		}
	}
	ob.LastUpdated = lastUpdatedTime
	s.obs[sc.Pair] = ob
	return nil
}

func (s *Kraken) handleOrderBookUpdate(sc *SubscriptionChannel, ad, bd []interface{}, checksum string) error {
	s.obsMutex.Lock()
	defer s.obsMutex.Unlock()

	ob, ok := s.obs[sc.Pair]
	if !ok {
		return fmt.Errorf("order book for %s not found", sc.Pair)
	}

	var lastUpdatedTime time.Time
	// Ask data is not always sent
	for i := range ad {
		asks, ok := ad[i].([]interface{})
		if !ok {
			return errors.New("type asset asks")
		}
		if len(asks) < 3 {
			return errors.New("invalid asks len")
		}
		price, quantity := asks[0].(string), asks[1].(string)

		value, err := strconv.ParseFloat(quantity, 64)
		if err != nil {
			return fmt.Errorf("parse quantity: %w", err)
		}
		if value == 0 {
			delete(ob.Asks, price)
			continue
		}

		item, ok := ob.Asks[price]
		if !ok {
			item = &OrderBookItem{
				Price:    price,
				Quantity: quantity,
			}
		} else {
			item.Quantity = quantity
		}
		ob.Asks[price] = item

		timestamp, err := strconv.ParseFloat(asks[2].(string), 64)
		if err != nil {
			return err
		}
		askUpdatedTime := timeFromUnixTimestampDecimal(timestamp)
		if lastUpdatedTime.Before(askUpdatedTime) {
			lastUpdatedTime = askUpdatedTime
		}
	}

	// Bid data is not always sent
	for i := range bd {
		bids, ok := bd[i].([]interface{})
		if !ok {
			return errors.New("type asset bids")
		}
		if len(bids) < 3 {
			return errors.New("invalid bids len")
		}
		price, quantity := bids[0].(string), bids[1].(string)

		value, err := strconv.ParseFloat(quantity, 64)
		if err != nil {
			return fmt.Errorf("parse quantity: %w", err)
		}
		if value == 0 {
			delete(ob.Bids, price)
			continue
		}

		item, ok := ob.Bids[price]
		if !ok {
			item = &OrderBookItem{
				Price:    price,
				Quantity: quantity,
			}
		} else {
			item.Quantity = quantity
		}
		ob.Bids[price] = item

		timestamp, err := strconv.ParseFloat(bids[2].(string), 64)
		if err != nil {
			return err
		}
		askUpdatedTime := timeFromUnixTimestampDecimal(timestamp)
		if lastUpdatedTime.Before(askUpdatedTime) {
			lastUpdatedTime = askUpdatedTime
		}
	}
	ob.LastUpdated = lastUpdatedTime

	token, err := strconv.ParseInt(checksum, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid checksum: %s", checksum)
	}

	if len(ob.Asks) < krakenChecksumCalculation || len(ob.Bids) < krakenChecksumCalculation {
		return nil
	}

	// the top ten ask price levels should be sorted by price from low to high.
	// the top ten bid price levels should be sorted by price from high to low.
	asks := sortOrderBook(ob.Asks, true)
	bids := sortOrderBook(ob.Bids, false)

	// validate the checksum of the update
	if !s.validateCRC32(asks[:krakenChecksumCalculation], bids[:krakenChecksumCalculation], uint32(token)) {
		return fmt.Errorf("%s: validate checksum failed", sc.Pair)
	}

	return nil
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

func (s *Kraken) validateCRC32(asks, bids []*OrderBookItem, token uint32) bool {
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

	return crc32.ChecksumIEEE([]byte(tokenBuilder.String())) == token
}

func (s *Kraken) handleOrderBook(sc *SubscriptionChannel, ob map[string]interface{}) error {
	as, ase := ob[krakenWsOrderbookAskSnapshot].([]interface{})
	bs, bse := ob[krakenWsOrderbookBidSnapshot].([]interface{})
	// if it's a snapshot payload
	if ase || bse {
		if err := s.handleOrderBookSnapshot(sc, as, bs); err != nil {
			return fmt.Errorf("snapshot: %w", err)
		}
		return nil
	}

	// if it's a update payload
	a, ae := ob[krakenWsOrderbookAsk].([]interface{})
	b, be := ob[krakenWsOrderbookBid].([]interface{})
	c, ce := ob[krakenWsOrderbookCheckum].(string)
	if !ce {
		return errors.New("checksum not found")
	}
	if ae || be {
		if err := s.handleOrderBookUpdate(sc, a, b, c); err != nil {
			// if needs to unsubscribe and subscribe again ?
			return fmt.Errorf("%s update: %w", sc.Pair, err)
		}
		return nil
	}
	return nil
}

func (s *Kraken) handlePublicMessage(data []byte) error {
	var fields []interface{}
	if err := json.Unmarshal(data, &fields); err != nil {
		return fmt.Errorf("json unmarshal: %w", err)
	}
	if len(fields) != 4 {
		return fmt.Errorf("invalid public message: %s", data)
	}
	pair, ok := fields[3].(string)
	if !ok {
		return fmt.Errorf("pair must be string: %v", fields[2])
	}
	channel := s.findSubscriptionChannel(pair)
	if channel == nil {
		return fmt.Errorf("asset pair is not subscribed: %s", pair)
	}

	switch channel.Subscription {
	case krakenWsOrderbook:
		ob, ok := fields[1].(map[string]interface{})
		if !ok {
			return fmt.Errorf("received invalid orderbook message: %s", data)
		}
		if err := s.handleOrderBook(channel, ob); err != nil {
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
		s.addSubscriptionChannel(&status)
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
		if err := s.handlePublicMessage(data); err != nil {
			return fmt.Errorf("handle public message: %w", err)
		}
	case '{': // handle the general message
		if err := s.handleGeneralMessage(data); err != nil {
			return fmt.Errorf("handle general message: %w", err)
		}
	default:
		return fmt.Errorf("unexpected message: %s", data)
	}
	return nil
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
