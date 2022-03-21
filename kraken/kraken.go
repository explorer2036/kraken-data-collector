package kraken

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/sirupsen/logrus"
)

const (
	krakenAPIURL      = "https://api.kraken.com"
	krakenAPIVersion  = "0"
	krakenAssetPairs  = "AssetPairs"
	krakenSnapshotDir = "data"
)

type Kraken struct {
	sync.Mutex

	conn *websocket.Conn

	obsMutex sync.RWMutex
	obs      map[string]*CachedOrderBook
}

func New() *Kraken {
	s := &Kraken{
		obs: make(map[string]*CachedOrderBook),
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
	os.MkdirAll(krakenSnapshotDir, os.ModePerm)

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
	if err := s.doSubscribe(krakenWsOrderbook, pairs[:]); err != nil {
		return fmt.Errorf("do subscribe: %w", err)
	}

	// snapshot the order book to disk periodly
	go s.snapshot2Disk(ctx)

	// block until received exit signal
	<-ctx.Done()

	return nil
}

func (s *Kraken) collectSnapshots() []*OrderBookSnapshot {
	s.obsMutex.RLock()
	defer s.obsMutex.RUnlock()

	var snapshots []*OrderBookSnapshot
	for pair, ob := range s.obs {
		total := ob.Success + ob.Failure
		if total > 0 {
			logrus.Infof("%s \t-> %d/%d(%.2f%%)", pair, ob.Success, total, float64(ob.Success)/float64(total)*100)
		}
		snapshot := OrderBookSnapshot{
			Pair:      pair,
			Timestamp: ob.LastUpdated,
			Body:      OrderBookBody{},
		}

		sortedAsks := sortOrderBook(ob.Asks, true)
		for _, ask := range sortedAsks {
			var group []string
			group = append(group, ask.Price)
			group = append(group, ask.Quantity)
			snapshot.Body.Asks = append(snapshot.Body.Asks, group)
		}

		sortedBids := sortOrderBook(ob.Bids, true)
		for _, bid := range sortedBids {
			var group []string
			group = append(group, bid.Price)
			group = append(group, bid.Quantity)
			snapshot.Body.Bids = append(snapshot.Body.Bids, group)
		}
		snapshots = append(snapshots, &snapshot)
	}
	return snapshots
}

func (s *Kraken) snapshot2Disk(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(time.Second)
		for {
			select {
			case <-ctx.Done():
				ticker.Stop()
				return
			case <-ticker.C:
				for _, snapshot := range s.collectSnapshots() {
					if err := sync2Disk(snapshot); err != nil {
						logrus.Errorf("sync snapshot to disk: %v", err)
					}
				}
			}
		}
	}()
}

func sync2Disk(snapshot *OrderBookSnapshot) error {
	data, err := json.Marshal(snapshot)
	if err != nil {
		return err
	}

	name := fmt.Sprintf("%s/%s.json", krakenSnapshotDir, strings.ToLower(strings.ReplaceAll(snapshot.Pair, "/", "-")))
	file, err := os.OpenFile(name, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	var buf bytes.Buffer
	buf.Write(data)
	buf.WriteByte('\n')
	if _, err := file.Write(buf.Bytes()); err != nil {
		return err
	}
	return nil
}

func (s *Kraken) Stop() {
	if s.conn != nil {
		s.conn.Close()
	}
}
