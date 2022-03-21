package kraken

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/sirupsen/logrus"
)

const (
	krakenAPIURL     = "https://api.kraken.com"
	krakenAPIVersion = "0"
	krakenAssetPairs = "AssetPairs"
)

type Kraken struct {
	sync.Mutex

	conn *websocket.Conn
	subs map[string]*SubscriptionChannel

	obsMutex sync.RWMutex
	obs      map[string]*OrderBook
}

func New() *Kraken {
	s := &Kraken{
		subs: make(map[string]*SubscriptionChannel),
		obs:  make(map[string]*OrderBook),
	}
	return s
}

// AssetPairs returns all asset pairs
func (k *Kraken) AssetPairs(ctx context.Context) (map[string]AssetPairs, error) {
	url := fmt.Sprintf("%s/%s/public/%s", krakenAPIURL, krakenAPIVersion, krakenAssetPairs)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var res struct {
		Error  []string              `json:"error"`
		Result map[string]AssetPairs `json:"result"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return nil, err
	}
	return res.Result, nil
}

func (s *Kraken) Start(ctx context.Context) error {
	// init the websocket connection to kraken exchange
	if err := s.initStream(ctx); err != nil {
		return fmt.Errorf("init websocket stream: %v", err)
	}

	// all asset pairs
	assetPairs, err := s.AssetPairs(ctx)
	if err != nil {
		return fmt.Errorf("all asset pairs: %v", err)
	}

	pairs := make([]string, 0, len(assetPairs))
	for _, item := range assetPairs {
		pairs = append(pairs, item.Wsname)
	}
	// subscribe the pairs
	if err := s.doSubscribe(krakenWsOrderbook, pairs[:2]); err != nil {
		return fmt.Errorf("do subscribe: %w", err)
	}

	// snapshot the order book to disk periodly
	// go s.snapshot2Disk(ctx)

	// block until received exit signal
	<-ctx.Done()

	return nil
}

type OrderBookHistory struct {
	Timestamp time.Time         `json:"timestamp"`
	Snapshot  OrderBookSnapshot `json:"snapshot"`
}

type OrderBookSnapshot struct {
	Asks []interface{} `json:"asks"`
	Bids []interface{} `json:"bids"`
}

func (s *Kraken) snapshot2Disk(ctx context.Context) {
	s.obsMutex.RLock()
	defer s.obsMutex.RUnlock()

	go func() {
		ticker := time.NewTicker(time.Second * 5)
		for {
			select {
			case <-ctx.Done():
				ticker.Stop()
				return
			case <-ticker.C:
				for _, ob := range s.obs {
					history := OrderBookHistory{
						Timestamp: ob.LastUpdated,
						Snapshot:  OrderBookSnapshot{},
					}

					sortedAsks := sortOrderBook(ob.Asks, true)
					for _, ask := range sortedAsks {
						var group []string
						group = append(group, ask.Price)
						group = append(group, ask.Quantity)
						history.Snapshot.Asks = append(history.Snapshot.Asks, group)
					}

					sortedBids := sortOrderBook(ob.Bids, true)
					for _, bid := range sortedBids {
						var group []string
						group = append(group, bid.Price)
						group = append(group, bid.Quantity)
						history.Snapshot.Bids = append(history.Snapshot.Bids, group)
					}

					b, _ := json.Marshal(history)
					logrus.Infof("history: %s", b)
				}
			}
		}
	}()
}

func (s *Kraken) Stop() {
	if s.conn != nil {
		s.conn.Close()
	}
}
