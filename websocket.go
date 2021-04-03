package kraken

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/buger/jsonparser"
	"github.com/gorilla/websocket"
	"github.com/nikandfor/errors"
	"github.com/nikandfor/tlog"
)

type (
	Websocket struct {
		c *websocket.Conn

		sys  chan interface{}
		ch   chan Event
		errc chan error

		mu   sync.Mutex
		reqs map[int64]chan []byte

		token string

		pairs  []string
		topics []string

		connid  Decimal
		version string

		stopc chan struct{}
	}

	Event struct {
		Channel string      `json:"channel,omitempty"`
		Pair    string      `json:"pair,omitempty"`
		Data    interface{} `json:"data,omitempty"`
	}
)

var ErrStopped = errors.New("stopped")

func (c *Client) Websocket(ctx context.Context) (s *Websocket, err error) {
	conn, _, err := (&websocket.Dialer{}).Dial(c.wsbase, nil)
	if err != nil {
		return nil, errors.Wrap(err, "dial")
	}

	var st struct {
		ConnID  Decimal `json:"connectionID"`
		Event   string  `json:"event"`
		Status  string  `json:"status"`
		Version string  `json:"version"`
	}

	err = conn.ReadJSON(&st)
	if err != nil {
		return nil, errors.Wrap(err, "read status")
	}

	if st.Status != "online" {
		return nil, errors.New("bad system status: %v", st.Status)
	}

	s = newSubscribtion(conn, "")

	s.connid = st.ConnID
	s.version = st.Version

	return s, nil
}

func (c *Client) PrivateWebsocket(ctx context.Context) (s *Websocket, err error) {
	var token struct {
		Token string `json:"token"`
	}

	err = c.callPrivate("GetWebSocketsToken", nil, &token)
	if err != nil {
		return nil, errors.Wrap(err, "get token")
	}

	conn, _, err := (&websocket.Dialer{}).Dial(c.wsbasePrivate, nil)
	if err != nil {
		return nil, errors.Wrap(err, "dial")
	}

	s = newSubscribtion(conn, token.Token)

	return s, nil
}

func newSubscribtion(c *websocket.Conn, token string) (s *Websocket) {
	s = &Websocket{
		c:     c,
		sys:   make(chan interface{}, 16),
		ch:    make(chan Event, 16),
		errc:  make(chan error, 1),
		reqs:  make(map[int64]chan []byte),
		token: token,
	}

	go s.reader()

	return s
}

func (s *Websocket) AddOrder(ctx context.Context, o Order) (txid string, err error) {
	reqid := time.Now().UnixNano()

	q := map[string]string{
		"event": "addOrder",
		"token": s.token,
		"reqid": fmt.Sprintf("%v", reqid),

		"ordertype": o.Type,
		"type":      o.Side.EncodeToString(),
		"pair":      o.Pair,
		"volume":    o.Volume,
	}

	if o.Price != "" {
		q["price"] = o.Price
	}

	for k, v := range o.Rest {
		q[k] = v
	}

	var res struct {
		Event  string `json:"event"`
		ReqID  int64  `json:"reqid"`
		Status string `json:"status"`
		TxID   string `json:"txid"`
		Descr  string `json:"descr"`
		Error  string `json:"errorMessage"`
	}

	err = s.request(ctx, reqid, q, &res)
	if err != nil {
		return "", err
	}

	if res.Error != "" {
		return "", errors.New(res.Error)
	}

	return res.TxID, nil
}

func (s *Websocket) CancelOrder(ctx context.Context, txid ...string) (err error) {
	if len(txid) == 0 {
		return nil
	}

	reqid := time.Now().UnixNano()

	q := map[string]interface{}{
		"event": "cancelOrder",
		"token": s.token,
		"reqid": fmt.Sprintf("%v", reqid),

		"txid": txid,
	}

	var res struct {
		Event  string `json:"event"`
		ReqID  int64  `json:"reqid"`
		Status string `json:"status"`
		Error  string `json:"errorMessage"`
	}

	err = s.request(ctx, reqid, q, &res)
	if err != nil {
		return err
	}

	if res.Error != "" {
		return errors.New(res.Error)
	}

	return nil
}

func (s *Websocket) Subscribe(ctx context.Context, topic string, pairs []string, args ...string) (err error) {
	reqid := time.Now().UnixNano()

	sc := map[string]string{
		"name": topic,
	}
	if s.token != "" {
		sc["token"] = s.token
	}

	for _, a := range args {
		p := strings.Index(a, "=")
		if p == -1 {
			sc[a] = ""
		} else {
			sc[a[:p]] = a[1+p:]
		}
	}

	e := map[string]interface{}{
		"event":        "subscribe",
		"subscription": sc,
		"reqid":        reqid,
	}

	if len(pairs) != 0 {
		e["pair"] = pairs
	}

	tlog.V("subscribe_request").Printw("subscribe", "req", e)

	var res struct {
		Status    string `json:"status"`
		Error     string `json:"errorMessage"`
		ChannelID int    `json:"channelID"`
		Pair      string `json:"pair"`
	}

	err = s.requestMany(ctx, reqid, e, &res, len(pairs), func() error {
		tlog.Printw("subscribed", "res", res)

		if res.Error != "" {
			return errors.New(res.Error)
		}

		return nil
	})
	if err != nil {
		return err
	}

	return nil
}

func (s *Websocket) request(ctx context.Context, reqid int64, req, resp interface{}) (err error) {
	resc := make(chan []byte, 1)

	defer s.waitReq(reqid, resc)()

	err = s.c.WriteJSON(req)
	if err != nil {
		return errors.Wrap(err, "write request")
	}

	var msg []byte
	select {
	case msg = <-resc:
	case <-ctx.Done():
		err = ctx.Err()
	case <-s.stopc:
		err = ErrStopped
	}
	if err != nil {
		return
	}

	err = json.Unmarshal(msg, resp)
	if err != nil {
		return errors.Wrap(err, "decode response")
	}

	return nil
}

func (s *Websocket) requestMany(ctx context.Context, reqid int64, req, resp interface{}, n int, cb func() error) (err error) {
	resc := make(chan []byte, 1)

	defer s.waitReq(reqid, resc)()

	err = s.c.WriteJSON(req)
	if err != nil {
		return errors.Wrap(err, "write request")
	}

	var msg []byte

	for i := 0; i < n; i++ {
		select {
		case msg = <-resc:
		case <-ctx.Done():
			err = ctx.Err()
		case <-s.stopc:
			err = ErrStopped
		}
		if err != nil {
			return
		}

		err = json.Unmarshal(msg, resp)
		if err != nil {
			return errors.Wrap(err, "decode response")
		}

		err = cb()
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Websocket) C() <-chan Event {
	return s.ch
}

func (s *Websocket) Sys() <-chan interface{} {
	return s.sys
}

func (s *Websocket) Err() <-chan error {
	return s.errc
}

func (s *Websocket) reader() {
	for {
		_, msg, err := s.c.ReadMessage()
		if err != nil {
			s.errc <- errors.Wrap(err, "read message")

			break
		}

		tlog.V("websocket_messages").Printw("websocket message", "msg", msg)

		_, tp, _, err := jsonparser.Get(msg)

		switch tp {
		case jsonparser.Object:
			err = s.decodeObj(msg)
		case jsonparser.Array:
			err = s.decodeArr(msg)
		default:
			err = errors.New("unexpected message: %s", msg)
		}

		if err != nil {
			s.errc <- errors.Wrap(err, "parse event (%s)", msg)

			break
		}
	}
}

func (s *Websocket) decodeArr(msg []byte) (err error) {
	v, tp, _, _ := jsonparser.Get(msg, "[2]")
	if tp != jsonparser.String {
		v, tp, _, _ = jsonparser.Get(msg, "[1]")
	}
	if tp != jsonparser.String {
		jsonparser.ArrayEach(msg, func(v []byte, tp jsonparser.ValueType, off int, err error) {
			tlog.Printw("arr el", "v", v, "tp", tp, "off", off, "err", err)
		})

		return errors.New("not parsed")
	}

	chname := string(v)

	switch {
	case chname == "trade":
		return s.decodeTrade(msg)
	case strings.HasPrefix(chname, "book-"):
		return s.decodeBook(msg)
	case chname == "openOrders":
		return s.decodeOrders(msg)
	}

	return errors.New("unsupported channel: %v", chname)
}

func (s *Websocket) decodeTrade(msg []byte) (err error) {
	el := -1
	var ts []Trade
	var pair string

	_, err = jsonparser.ArrayEach(msg, func(v []byte, tp jsonparser.ValueType, off int, err error) {
		if err != nil {
			return
		}

		el++

		switch el {
		case 0:
			// chid
		case 1:
			// trades
			err = json.Unmarshal(v, &ts)
		case 2:
			// chname
		case 3:
			// pair
			pair = string(v)
		}
	})

	if err != nil {
		return
	}

	for i := range ts {
		ts[i].Pair = pair
	}

	select {
	case s.ch <- Event{
		Channel: "trade",
		Pair:    pair,
		Data:    ts,
	}:
	case <-s.stopc:
		return ErrStopped
	}

	return nil
}

func (s *Websocket) decodeBook(msg []byte) (err error) {
	el := -1
	var r Depth
	var chname string
	var pair string

	_, err = jsonparser.ArrayEach(msg, func(v []byte, tp jsonparser.ValueType, off int, err error) {
		if err != nil {
			return
		}

		el++

		switch el {
		case 0:
			// chid
		case 1:
			// trades
			err = json.Unmarshal(v, &r)
		case 2:
			// chname
			chname = string(v)
		case 3:
			// pair
			pair = string(v)
		}
	})

	if err != nil {
		return
	}

	select {
	case s.ch <- Event{
		Channel: chname,
		Pair:    pair,
		Data:    r,
	}:
	case <-s.stopc:
		return ErrStopped
	}

	return nil
}

func (s *Websocket) decodeOrders(msg []byte) (err error) {
	el := -1
	var r []Order
	var chname string

	_, err = jsonparser.ArrayEach(msg, func(v []byte, tp jsonparser.ValueType, off int, err error) {
		if err != nil {
			return
		}

		el++

		switch el {
		case 0:
			// orders
			err = json.Unmarshal(v, &r)
		case 1:
			// chname
			chname = string(v)
		case 2:
			// sequence
		}
	})

	if err != nil {
		return
	}

	select {
	case s.ch <- Event{
		Channel: chname,
		Data:    r,
	}:
	case <-s.stopc:
		return ErrStopped
	}

	return nil
}

func (s *Websocket) decodeObj(msg []byte) (err error) {
	var event string

	v, tp, _, _ := jsonparser.Get(msg, "reqid")
	if tp == jsonparser.Number {
		reqid, err := jsonparser.ParseInt(v)
		if err == nil {
			s.mu.Lock()
			c, ok := s.reqs[reqid]
			s.mu.Unlock()

			if ok {
				select {
				case c <- msg:
				case <-s.stopc:
					return ErrStopped
				}

				return nil
			}
		}
	}

	v, tp, _, _ = jsonparser.Get(msg, "event")
	if tp != jsonparser.String {
		event = string(v)
	}

	switch event {
	case "heartbeat":
		// drop
	default:
		select {
		case s.sys <- json.RawMessage(msg):
		default:
		}
	}

	return nil
}

func (s *Websocket) waitReq(id int64, c chan []byte) func() {
	s.mu.Lock()

	_, ok := s.reqs[id]

	if !ok {
		s.reqs[id] = c
	}

	s.mu.Unlock()

	if ok {
		panic("duplicated reqid")
	}

	return func() {
		s.mu.Lock()
		delete(s.reqs, id)
		s.mu.Unlock()
	}
}
