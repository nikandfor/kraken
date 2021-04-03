package kraken

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"github.com/buger/jsonparser"
	"github.com/nikandfor/errors"
	"github.com/nikandfor/tlog"
	"github.com/shopspring/decimal"
)

type (
	Decimal = decimal.Decimal

	Side bool

	Client struct {
		base          string
		wsbase        string
		wsbasePrivate string

		key    string
		secret []byte

		nonce int64

		cl *http.Client

		// rate limit
		callsLimit    int
		callsLastTime time.Time

		UserAgent string
	}

	RemoteError string

	RemoteErrors []RemoteError

	SystemStatus struct {
		Status    string    `json:"status"`
		Timestamp time.Time `json:"timestamp"`
	}

	Asset struct {
		Name            string `json:"name"`
		AltName         string `json:"altname"`
		AssetClass      string `json:"aclass"`
		Decimals        int    `json:"decimals"`
		DisplayDecimals int    `json:"display_decimals"`
	}

	Fees []struct {
		Volume Decimal `json:"volume"`
		Fee    Decimal `json:"fee"`
	}

	AssetPair struct {
		Name              string  `json:"name"`
		AltName           string  `json:"altname,omitempty"`
		WSName            string  `json:"wsname,omitempty"`
		AssetClassBase    string  `json:"aclass_base,omitempty"`
		Base              string  `json:"base,omitempty"`
		AssetClassQuote   string  `json:"aclass_quote,omitempty"`
		Quote             string  `json:"quote,omitempty"`
		Lot               string  `json:"lot,omitempty"`
		PairDecimals      int     `json:"pair_decimals,omitempty"`
		LotDecimals       int     `json:"lot_decimals,omitempty"`
		LotMultiplier     int     `json:"lot_multiplier,omitempty"`
		LeverageBuy       []int   `json:"leverage_buy,omitempty"`
		LeverageSell      []int   `json:"leverage_sell,omitempty"`
		Fees              Fees    `json:"fees,omitempty"`
		FeesMaker         Fees    `json:"fees_maker,omitempty"`
		FeeVolumeCurrency string  `json:"fee_volume_currency,omitempty"`
		MarginCall        int     `json:"margin_call,omitempty"`
		MarginStop        int     `json:"margin_stop,omitempty"`
		MarginLevel       int     `json:"margin_level,omitempty"`
		OrderMin          Decimal `json:"ordermin,omitempty"`
	}

	VolumeSummary struct {
		Today       Decimal
		Last24Hours Decimal
	}

	PriceSummary = VolumeSummary

	NumberSummary struct {
		Today       int64
		Last24Hours int64
	}

	Ticker struct {
		AskPrice          Decimal
		AskLotVolume      Decimal
		AskWholeLotVolume Decimal

		BidPrice          Decimal
		BidLotVolume      Decimal
		BidWholeLotVolume Decimal

		LastPrice  Decimal
		LastVolume Decimal

		Volume      VolumeSummary
		TotalTrades NumberSummary

		Weighted PriceSummary
		Low      PriceSummary
		High     PriceSummary

		TodayOpeningPrice Decimal
	}

	Candle struct {
		Time   int64   `json:"time"`
		Open   Decimal `json:"open"`
		High   Decimal `json:"high"`
		Low    Decimal `json:"low"`
		Close  Decimal `json:"close"`
		VWap   Decimal `json:"vwap"`
		Volume Decimal `json:"volume"`
		Count  int64   `json:"count"`
	}

	pageResp struct {
		r    interface{}
		Last string
	}

	PriceLevel struct {
		Price     Decimal `json:"price"`
		Volume    Decimal `json:"volume"`
		Timestamp int64   `json:"timestamp"`
	}

	Depth struct {
		Asks []PriceLevel `json:"asks"`
		Bids []PriceLevel `json:"bids"`
		C    Decimal      `json:"c"`
	}

	response struct {
		Errors RemoteErrors `json:"error"`
		Result interface{}  `json:"result"`
	}

	Trade struct {
		Pair      string  `json:"pair"`
		Price     Decimal `json:"price"`
		Volume    Decimal `json:"volume"`
		Timestamp int64   `json:"time"`
		Side      Side    `json:"side"` // buy/sell
		Type      string  `json:"type"`

		Miscellaneous string `json:"miscellaneous,omitempty"`
	}

	Spread struct {
		Time int64   `json:"time"`
		Bid  Decimal `json:"bid"`
		Ask  Decimal `json:"ask"`
	}

	TradeBalance struct {
		Equivalent  Decimal `json:"equivalent"`
		Trade       Decimal `json:"trade"`
		Margin      Decimal `json:"margin"`
		Unrealized  Decimal `json:"unrealized"`
		Cost        Decimal `json:"cost"`
		Valuation   Decimal `json:"valuation"`
		Equity      Decimal `json:"equity"`
		MarginFree  Decimal `json:"margin_free"`
		MarginLevel Decimal `json:"margin_level"`
	}

	Order struct {
		TxID   string `json:"txid,omitempty"`
		Status string `json:"status,omitempty"`
		Pair   string `json:"pair"`
		Side   Side   `json:"side"` // buy/sell
		Type   string `json:"type"` // market/limit
		Price  string `json:"price"`
		Volume string `json:"volume"`

		Rest map[string]string `json:"rest"`
	}
)

const (
	Buy  Side = true
	Sell Side = false
)

var ns = decimal.New(1, 9) // seconds to nanoseconds

func New(key, secret string) (*Client, error) {
	s, err := base64.StdEncoding.DecodeString(secret)
	if err != nil {
		return nil, errors.Wrap(err, "decode secret")
	}

	c := &Client{
		base:          "https://api.kraken.com",
		wsbase:        "wss://ws.kraken.com",
		wsbasePrivate: "wss://ws-auth.kraken.com",
		key:           key,
		secret:        s,
		nonce:         time.Now().UnixNano(),
		cl:            http.DefaultClient,
		UserAgent:     "github.com/nikandfor/kraken",
	}

	return c, nil
}

// public market data

func (c *Client) Time() (t time.Time, err error) {
	var r struct {
		Unix    float64 `json:"unixtime"`
		RFC1123 string  `json:"rfc1123"`
	}

	err = c.callPublic("Time", "", &r)
	if err != nil {
		return t, err
	}

	t = time.Unix(int64(r.Unix), int64((r.Unix-float64(int64(r.Unix)))*1e9)).UTC()

	//	tlog.Printw("time", "time", t, "response", r)

	return t, nil
}

func (c *Client) SystemStatus() (s SystemStatus, err error) {
	err = c.callPublic("SystemStatus", "", &s)
	if err != nil {
		return s, err
	}

	return s, nil
}

func (c *Client) Assets() (r map[string]Asset, err error) {
	err = c.callPublic("Assets", "", &r)
	if err != nil {
		return r, err
	}

	for n, a := range r {
		a.Name = n
	}

	return r, nil
}

func (c *Client) AssetPairs(pairs []string, info string) (r map[string]AssetPair, err error) {
	p := "AssetPairs"
	q := ""

	if info != "" || len(pairs) != 0 {
		v := url.Values{}

		if info != "" {
			v.Set("info", info)
		}

		if len(pairs) != 0 {
			v.Set("pair", strings.Join(pairs, ","))
		}

		q = v.Encode()
	}

	err = c.callPublic(p, q, &r)
	if err != nil {
		return r, err
	}

	for n, a := range r {
		a.Name = n
	}

	return r, nil
}

func (c *Client) Ticker(pairs []string) (r map[string]Ticker, err error) {
	p := "Ticker"
	q := ""

	if len(pairs) != 0 {
		v := url.Values{}

		v.Set("pair", strings.Join(pairs, ","))

		q = v.Encode()
	}

	err = c.callPublic(p, q, &r)
	if err != nil {
		return r, err
	}

	return r, nil
}

func (c *Client) Candles(pair string, interval time.Duration, since string) (cs []Candle, last string, err error) {
	p := "OHLC"

	v := url.Values{}

	v.Set("pair", pair)

	if interval != 0 {
		v.Set("interval", fmt.Sprintf("%d", int(interval.Round(time.Minute)/time.Minute)))
	}

	if since != "" {
		v.Set("since", since)
	}

	q := v.Encode()

	r := pageResp{
		r: &cs,
	}

	err = c.callPublic(p, q, &r)
	if err != nil {
		return nil, "", err
	}

	return cs, r.Last, nil
}

func (c *Client) Depth(pair string, cnt int) (d Depth, err error) {
	p := "Depth"

	v := url.Values{}

	v.Set("pair", pair)

	if cnt >= 0 {
		v.Set("count", fmt.Sprintf("%d", cnt))
	}

	q := v.Encode()

	r := pageResp{
		r: &d,
	}

	err = c.callPublic(p, q, &r)
	if err != nil {
		return Depth{}, err
	}

	return d, nil
}

func (c *Client) Trades(pair string, since string) (ts []Trade, last string, err error) {
	p := "Trades"

	v := url.Values{}

	v.Set("pair", pair)

	if since != "" {
		v.Set("since", since)
	}

	q := v.Encode()

	r := pageResp{
		r: &ts,
	}

	err = c.callPublic(p, q, &r)
	if err != nil {
		return ts, "", err
	}

	for i := range ts {
		ts[i].Pair = pair
	}

	return ts, r.Last, nil
}

func (c *Client) Spread(pair string, since string) (ss []Spread, last string, err error) {
	p := "Spread"

	v := url.Values{}

	v.Set("pair", pair)

	if since != "" {
		v.Set("since", since)
	}

	q := v.Encode()

	r := pageResp{
		r: &ss,
	}

	err = c.callPublic(p, q, &r)
	if err != nil {
		return ss, "", err
	}

	return ss, r.Last, nil
}

// private user data

func (c *Client) Balance() (r map[string]Decimal, err error) {
	err = c.callPrivate("Balance", nil, &r)
	if err != nil {
		return nil, err
	}

	return r, nil
}

func (c *Client) TradeBalance(asset, aclass string) (r TradeBalance, err error) {
	v := url.Values{}

	v.Set("asset", asset)

	if aclass != "" {
		v.Set("aclass", aclass)
	}

	err = c.callPrivate("TradeBalance", v, &r)
	if err != nil {
		return TradeBalance{}, err
	}

	return r, nil
}

func (c *Client) OpenOrders(trades bool, userref string) (r json.RawMessage, err error) {
	v := url.Values{}

	if trades {
		v.Set("trades", "true")
	}

	if userref != "" {
		v.Set("userref", userref)
	}

	err = c.callPrivate("OpenOrders", v, &r)
	if err != nil {
		return nil, err
	}

	return r, nil
}

// ClosedOrders
// OrdersByID
// Trades
// TradesByID

// private trading API

func (c *Client) AddOrder() (r json.RawMessage, err error) {
	v := url.Values{}

	v.Set("pair", "")

	err = c.callPrivate("AddOrder", v, &r)
	if err != nil {
		return nil, err
	}

	return r, nil
}

// helpers

func (c *Client) callPublic(pth, q string, res interface{}) (err error) {
	return c.call(http.MethodGet, "/0/public/"+pth+qq(q), nil, nil, res)
}

func (c *Client) callPrivate(pth string, body url.Values, res interface{}) (err error) {
	nonce := c.nextNonce()
	pth = "/0/private/" + pth

	if body == nil {
		body = url.Values{}
	}

	body.Set("nonce", nonce)

	encoded := body.Encode()

	h := hmac.New(sha512.New, c.secret)
	_, _ = h.Write([]byte(pth))

	bh := sha256.Sum256([]byte(nonce + encoded))
	_, _ = h.Write(bh[:])

	sig := h.Sum(nil)
	sigs := base64.StdEncoding.EncodeToString(sig)

	hdr := http.Header{}

	hdr.Set("API-Key", c.key)
	hdr.Set("API-Sign", sigs)

	return c.call(http.MethodPost, pth, hdr, strings.NewReader(encoded), res)
}

func (c *Client) call(meth, pth string, hdr http.Header, reqbody, res interface{}) (err error) {
	var body io.Reader

	switch b := reqbody.(type) {
	case nil:
	case io.Reader:
		body = b
	case []byte:
		body = bytes.NewReader(b)
	default:
		return errors.New("unsupported body type: %T", b)
	}

	req, err := http.NewRequest(meth, c.base+pth, body)
	if err != nil {
		return errors.Wrap(err, "new request")
	}

	req.Header.Set("User-Agent", c.UserAgent)

	for k, v := range hdr {
		req.Header[k] = v
	}

	if tlog.If("rawrequest") {
		var data []byte

		if body != nil {
			data, err = io.ReadAll(body)
			if err != nil {
				return errors.Wrap(err, "read body")
			}

			req.Body = io.NopCloser(bytes.NewReader(data))
		}

		w := tlog.DefaultLogger.IOWriter(1)

		err = req.Write(w)
		if err != nil {
			return errors.Wrap(err, "encode request")
		}

		if data != nil {
			req.Body = io.NopCloser(bytes.NewReader(data))
		}
	}

	resp, err := c.cl.Do(req)
	if err != nil {
		return errors.Wrap(err, "do")
	}

	//	if resp.StatusCode != http.StatusOK {
	//		return errors.New("bad status: %v (%v)", resp.Status, resp.StatusCode)
	//	}

	var buf bytes.Buffer
	var rd io.Reader = resp.Body

	if tlog.If("rawresponse") {
		rd = io.TeeReader(resp.Body, &buf)
	}

	d := json.NewDecoder(rd)
	defer func() {
		e := resp.Body.Close()
		if err == nil {
			err = e
		}
	}()

	r := response{
		Result: res,
	}

	err = d.Decode(&r)
	if err != nil || tlog.If("rawresponse") {
		tlog.Printw("decode", "err", err, "data", buf.String())
	}
	if err != nil {
		return errors.Wrap(err, "decode response")
	}

	if len(r.Errors) != 0 {
		return r.Errors
	}

	return nil
}

func (c *Client) nextNonce() string {
	n := atomic.AddInt64(&c.nonce, 1)

	return fmt.Sprintf("%d", n)
}

func (e RemoteError) Error() string {
	return string(e)
}

func (e RemoteErrors) Error() string {
	switch len(e) {
	case 0:
		return "no errors"
	case 1:
		return e[0].Error()
	default:
		return e[0].Error() + fmt.Sprintf(" and %d more errors", len(e)-1)
	}
}

func (f *Fees) UnmarshalJSON(data []byte) (err error) {
	var q [][2]Decimal

	err = json.Unmarshal(data, &q)
	if err != nil {
		return
	}

	*f = make(Fees, len(q))

	for i, p := range q {
		(*f)[i].Volume = p[0]
		(*f)[i].Fee = p[1]
	}

	return nil
}

func (t *Ticker) UnmarshalJSON(data []byte) (err error) {
	var q struct {
		A [3]Decimal `json:"a"`
		B [3]Decimal `json:"b"`
		C [2]Decimal `json:"c"`
		V [2]Decimal `json:"v"`
		P [2]Decimal `json:"p"`
		T [2]int64   `json:"t"`
		H [2]Decimal `json:"h"`
		L [2]Decimal `json:"l"`
		O Decimal    `json:"o"`
	}

	err = json.Unmarshal(data, &q)
	if err != nil {
		return err
	}

	*t = Ticker{
		AskPrice:          q.A[0],
		AskWholeLotVolume: q.A[1],
		AskLotVolume:      q.A[2],

		BidPrice:          q.B[0],
		BidWholeLotVolume: q.B[1],
		BidLotVolume:      q.B[2],

		LastPrice:  q.C[0],
		LastVolume: q.C[1],

		Volume: VolumeSummary{
			Today:       q.V[0],
			Last24Hours: q.V[1],
		},

		TotalTrades: NumberSummary{
			Today:       q.T[0],
			Last24Hours: q.T[1],
		},

		Weighted: VolumeSummary{
			Today:       q.P[0],
			Last24Hours: q.P[1],
		},

		Low: VolumeSummary{
			Today:       q.L[0],
			Last24Hours: q.L[1],
		},

		High: VolumeSummary{
			Today:       q.H[0],
			Last24Hours: q.H[1],
		},

		TodayOpeningPrice: q.O,
	}

	return nil
}

func (c *Candle) UnmarshalJSON(data []byte) (err error) {
	var q []Decimal

	err = json.Unmarshal(data, &q)
	if err != nil {
		return err
	}

	if len(q) != 8 {
		return errors.New("unsupported candle format: expected 8 elements got %v", len(q))
	}

	*c = Candle{
		Time:   q[0].Mul(ns).IntPart(),
		Open:   q[1],
		High:   q[2],
		Low:    q[3],
		Close:  q[4],
		VWap:   q[5],
		Volume: q[6],
		Count:  q[7].IntPart(),
	}

	return nil
}

func (t *Trade) UnmarshalJSON(data []byte) (err error) {
	var q []interface{}

	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()

	err = d.Decode(&q)
	if err != nil {
		return err
	}

	if len(q) != 6 {
		return errors.New("unsupported trade format: expected 6 elements got %v", len(q))
	}

	err = t.Price.UnmarshalText([]byte(q[0].(string)))
	if err != nil {
		return errors.Wrap(err, "price")
	}

	err = t.Volume.UnmarshalText([]byte(q[1].(string)))
	if err != nil {
		return errors.Wrap(err, "volume")
	}

	var ts Decimal
	err = ts.UnmarshalText([]byte(q[2].(string)))
	if err != nil {
		return errors.Wrap(err, "time")
	}
	t.Timestamp = ts.Mul(ns).IntPart()

	switch q[3].(string) {
	case "b":
		t.Side = Buy
	case "s":
		t.Side = Sell
	default:
		return errors.New("unexpected side: %v", q[3])
	}

	t.Type = q[4].(string)

	t.Miscellaneous = q[5].(string)

	return nil
}

func (s *Spread) UnmarshalJSON(data []byte) (err error) {
	var q []Decimal

	err = json.Unmarshal(data, &q)
	if err != nil {
		return err
	}

	if len(q) != 3 {
		return errors.New("unsupported spread format: expected 3 elements got %v", len(q))
	}

	*s = Spread{
		Time: q[0].Mul(ns).IntPart(),
		Bid:  q[1],
		Ask:  q[2],
	}

	return nil
}

func (d *Depth) UnmarshalJSON(data []byte) (err error) {
	err = jsonparser.ObjectEach(data, func(k, v []byte, tp jsonparser.ValueType, off int) (err error) {
		if len(k) == 0 {
			return errors.New("empty key")
		}

		switch k[0] {
		case 'a':
			err = json.Unmarshal(v, &d.Asks)
		case 'b':
			err = json.Unmarshal(v, &d.Bids)
		case 'c':
			err = json.Unmarshal(v, &d.C)
		default:
			//	tlog.Printw("depth", "key", k, "value", v)
		}

		return err
	})
	return
}

func (o *Order) UnmarshalJSON(data []byte) (err error) {
	var more bool

	err = jsonparser.ObjectEach(data, func(k, v []byte, tp jsonparser.ValueType, off int) error {
		if more {
			return errors.New("unexpected key: %s", k)
		}

		more = true

		o.TxID = string(k)

		return jsonparser.ObjectEach(v, func(k, v []byte, tp jsonparser.ValueType, off int) error {
			switch string(k) {
			case "status":
				o.Status = string(v)
			case "vol":
				o.Volume = string(v)
			case "descr":
				return jsonparser.ObjectEach(v, func(k, v []byte, tp jsonparser.ValueType, off int) error {
					switch string(k) {
					case "pair":
						o.Pair = string(v)
					case "type":
						switch string(v) {
						case "b", "buy":
							o.Side = Buy
						case "s", "sell":
							o.Side = Sell
						default:
							return errors.New("order side: %s", v)
						}
					case "ordertype":
						o.Type = string(v)
					case "price":
						o.Price = string(v)
					default:
						o.Rest[string(k)] = string(v)
					}

					return nil
				})
			default:
				o.Rest[string(k)] = string(v)
			}

			return nil
		})
	})

	if err != nil {
		return err
	}

	return nil
}

func (r *pageResp) UnmarshalJSON(data []byte) (err error) {
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()

	tk, err := d.Token()
	if err != nil {
		return err
	}

	if tk != json.Delim('{') {
		return errors.New("object expected, got %v", tk)
	}

	for d.More() {
		tk, err := d.Token()
		if tk == json.Delim('}') {
			return nil
		}
		if err == io.EOF {
			return io.ErrUnexpectedEOF
		}
		if err != nil {
			return err
		}

		if tk == "last" {
			tk, err = d.Token()
			if err != nil {
				return err
			}

			switch tk := tk.(type) {
			case string:
				r.Last = tk
			case json.Number:
				r.Last = string(tk)
			default:
				return errors.New("unsupported last type: %T", tk)
			}

			continue
		}

		name, ok := tk.(string)
		if !ok {
			return errors.New("object key expected, got %v", tk)
		}

		err = d.Decode(r.r)
		if err != nil {
			return errors.New("%v: %v", name, err)
		}
	}

	return nil
}

func (l *PriceLevel) UnmarshalJSON(data []byte) (err error) {
	var q [3]Decimal

	err = json.Unmarshal(data, &q)
	if err != nil {
		return err
	}

	l.Price = q[0]
	l.Volume = q[1]
	l.Timestamp = q[2].Mul(ns).IntPart()

	return nil
}

func (b *TradeBalance) UnmarshalJSON(data []byte) (err error) {
	var q struct {
		EB Decimal `json:"eb"`
		TB Decimal `json:"tb"`
		M  Decimal `json:"m"`
		N  Decimal `json:"n"`
		C  Decimal `json:"c"`
		V  Decimal `json:"v"`
		E  Decimal `json:"e"`
		MF Decimal `json:"mf"`
		ML Decimal `json:"ml"`
	}

	err = json.Unmarshal(data, &q)
	if err != nil {
		return err
	}

	b.Equivalent = q.EB
	b.Trade = q.TB
	b.Margin = q.M
	b.Unrealized = q.N
	b.Cost = q.C
	b.Valuation = q.V
	b.Equity = q.E
	b.MarginFree = q.MF
	b.MarginLevel = q.ML

	return nil
}

func qq(q string) string {
	if q == "" {
		return ""
	}

	return "?" + q
}

func (s Side) EncodeToString() string {
	switch s {
	case Buy:
		return "b"
	case Sell:
		return "s"
	}

	return ""
}
