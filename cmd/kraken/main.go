package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/nikandfor/cli"
	"github.com/nikandfor/errors"
	"github.com/nikandfor/kraken"
	"github.com/nikandfor/tlog"
	"github.com/nikandfor/tlog/ext/tlflag"

	krakenapi "github.com/beldur/kraken-go-api-client"
)

func main() {
	callcmds := []*cli.Command{
		{
			Name:   "time,server_time",
			Action: serverTime,
		},
		{
			Name:   "status",
			Action: systemStatus,
		},
		{
			Name:   "assets",
			Action: assets,
		},
		{
			Name:   "assetpairs",
			Action: assetPairs,
			Flags: []*cli.Flag{
				cli.NewFlag("pair", "", "comma separated list of pairs to request"),
				cli.NewFlag("info", "", "info to request (info|leverage|fees|margin)"),
			},
		},
		{
			Name:   "ticker",
			Action: ticker,
			Flags: []*cli.Flag{
				cli.NewFlag("pair", "", "comma separated list of pairs to request"),
			},
		},
		{
			Name:   "candles,ohlc",
			Action: candles,
			Flags: []*cli.Flag{
				cli.NewFlag("pair", "", "pair"),
				cli.NewFlag("interval", time.Minute, "interval"),
				cli.NewFlag("since", "", "cursor"),
				cli.NewFlag("hide-cursor", false, "do not show response cursor"),
			},
		},
		{
			Name:   "depth",
			Action: depth,
			Flags: []*cli.Flag{
				cli.NewFlag("pair", "", "pair"),
				cli.NewFlag("count,max", 0, "max number of entries. 0 for all"),
			},
		},
		{
			Name:   "trades",
			Action: trades,
			Flags: []*cli.Flag{
				cli.NewFlag("pair", "", "pair"),
				cli.NewFlag("since", "", "cursor"),
				cli.NewFlag("hide-cursor", false, "do not show response cursor"),
			},
		},
		{
			Name:   "spread",
			Action: spread,
			Flags: []*cli.Flag{
				cli.NewFlag("pair", "", "pair"),
				cli.NewFlag("since", "", "cursor"),
				cli.NewFlag("hide-cursor", false, "do not show response cursor"),
			},
		},
		{
			Name:   "balance",
			Action: balance,
		},
		{
			Name:   "trade_balance",
			Action: tradeBalance,
			Flags: []*cli.Flag{
				cli.NewFlag("asset", "ZUSD", "asset"),
				cli.NewFlag("asset-class", "", "aclass"),
			},
		},
		{
			Name:   "open_orders,orders",
			Action: openOrders,
			Flags: []*cli.Flag{
				cli.NewFlag("trades", false, "include trades"),
				cli.NewFlag("userref", "", "user filter"),
			},
		},
		{
			Name:   "test",
			Action: test,
		}}

	cli.App = cli.Command{
		Name:   "kraken exchange tool",
		Before: before,
		Flags: []*cli.Flag{
			cli.NewFlag("format,fmt,f", "", "output format (json|empty for go default format)"),
			cli.NewFlag("user-agent", "github.com/nikandfor/kraken", "user-agent"),
			cli.NewFlag("key", "", "api key"),
			cli.NewFlag("secret", "", "api key secret"),
			cli.NewFlag("log", "stderr:dm", "log destination"),
			cli.NewFlag("v", "", "verbosity topics"),
			cli.NewFlag("debug", "", "debug addr to listen to"),
		},
		Commands: []*cli.Command{{
			Name:     "call",
			Before:   beforeCall,
			Commands: callcmds,
		}, {
			Name:   "subscribe",
			Before: beforeCall,
			Action: subscribe,
			Flags: []*cli.Flag{
				cli.NewFlag("pair", "", "comma separated list of pairs to subscribe"),
				cli.NewFlag("topic", "", "comma separated list of topics to subscribe"),
				cli.NewFlag("private", false, "use private endpoint"),
			},
		}},
	}

	cli.RunAndExit(os.Args)
}

var cl *kraken.Client

func before(c *cli.Command) error {
	w, err := tlflag.OpenWriter(c.String("log"))
	if err != nil {
		return errors.Wrap(err, "parse log flag")
	}

	tlog.DefaultLogger = tlog.New(w)

	tlog.SetFilter(c.String("v"))

	ls := tlog.FillLabelsWithDefaults("service=kraken", "_hostname", "_runid", "_execmd5")

	tlog.SetLabels(ls)

	if a := c.String("debug"); a != "" {
		runtime.SetBlockProfileRate(1)
		runtime.SetMutexProfileFraction(1)

		go func() {
			err := http.ListenAndServe(a, nil)
			tlog.Printw("debug server", "err", err)
			os.Exit(1)
		}()

		tlog.Printf("listen debug server on %v", a)
	}

	switch f := c.String("format"); f {
	case "", "json":
	default:
		return errors.New("unsupported output format: %v", f)
	}

	return nil
}

func beforeCall(c *cli.Command) (err error) {
	cl, err = newClient(c)

	return
}

func serverTime(c *cli.Command) error {
	t, err := cl.Time()
	if err != nil {
		return errors.Wrap(err, "server time")
	}

	return out(c, t)
}

func systemStatus(c *cli.Command) error {
	s, err := cl.SystemStatus()
	if err != nil {
		return errors.Wrap(err, "system status")
	}

	return out(c, s)
}

func assets(c *cli.Command) error {
	r, err := cl.Assets()
	if err != nil {
		return errors.Wrap(err, "assets")
	}

	return out(c, r)
}

func assetPairs(c *cli.Command) error {
	var pair []string
	if q := c.String("pair"); q != "" {
		pair = strings.Split(q, ",")
	}

	r, err := cl.AssetPairs(pair, c.String("info"))
	if err != nil {
		return errors.Wrap(err, "asset pairs")
	}

	return out(c, r)
}

func ticker(c *cli.Command) error {
	var pair []string
	if q := c.String("pair"); q != "" {
		pair = strings.Split(q, ",")
	}

	r, err := cl.Ticker(pair)
	if err != nil {
		return errors.Wrap(err, "ticker")
	}

	return out(c, r)
}

func candles(c *cli.Command) (err error) {
	cs, last, err := cl.Candles(c.String("pair"), c.Duration("interval"), c.String("since"))
	if err != nil {
		return errors.Wrap(err, "candles")
	}

	err = out(c, cs)
	if err != nil {
		return
	}

	if !c.Bool("hide-cursor") {
		err = out(c, last)
		if err != nil {
			return
		}
	}

	return nil
}

func depth(c *cli.Command) (err error) {
	r, err := cl.Depth(c.String("pair"), c.Int("count"))
	if err != nil {
		return errors.Wrap(err, "depth")
	}

	return out(c, r)
}

func trades(c *cli.Command) (err error) {
	ts, last, err := cl.Trades(c.String("pair"), c.String("since"))
	if err != nil {
		return errors.Wrap(err, "trades")
	}

	err = out(c, ts)
	if err != nil {
		return
	}

	if !c.Bool("hide-cursor") {
		err = out(c, last)
		if err != nil {
			return
		}
	}

	return nil
}

func spread(c *cli.Command) (err error) {
	ts, last, err := cl.Spread(c.String("pair"), c.String("since"))
	if err != nil {
		return errors.Wrap(err, "spread")
	}

	err = out(c, ts)
	if err != nil {
		return
	}

	if !c.Bool("hide-cursor") {
		err = out(c, last)
		if err != nil {
			return
		}
	}

	return nil
}

func balance(c *cli.Command) (err error) {
	bs, err := cl.Balance()
	if err != nil {
		return errors.Wrap(err, "balance")
	}

	return out(c, bs)
}

func tradeBalance(c *cli.Command) (err error) {
	bs, err := cl.TradeBalance(c.String("asset"), c.String("asset-class"))
	if err != nil {
		return errors.Wrap(err, "trade balance")
	}

	return out(c, bs)
}

func openOrders(c *cli.Command) (err error) {
	r, err := cl.OpenOrders(c.Bool("trades"), c.String("userref"))
	if err != nil {
		return errors.Wrap(err, "trade balance")
	}

	return out(c, r)
}

func subscribe(c *cli.Command) (err error) {
	ctx := context.Background()

	topic := c.String("topic")
	var pairs []string
	if q := c.String("pair"); q != "" {
		pairs = strings.Split(q, ",")
	}

	var ws *kraken.Websocket
	if c.Bool("private") {
		ws, err = cl.PrivateWebsocket(ctx)
	} else {
		ws, err = cl.Websocket(ctx)
	}
	if err != nil {
		return errors.Wrap(err, "connect")
	}

	err = ws.Subscribe(context.Background(), topic, pairs)
	if err != nil {
		return errors.Wrap(err, "subscribe")
	}

	var msg interface{}

	for {
		select {
		case msg = <-ws.Sys():
			tlog.V("sys").Printw("system msg", "msg", msg)
			continue
		case msg = <-ws.C():
		case err = <-ws.Err():
			return errors.Wrap(err, "subscription")
		}

		err = out(c, msg)
		if err != nil {
			return errors.Wrap(err, "out")
		}
	}

	return nil
}

func out(c *cli.Command, v interface{}) (err error) {
	switch c.String("format") {
	case "json":
		e := json.NewEncoder(os.Stdout)
		err = e.Encode(v)
	default:
		fmt.Printf("%v\n", v)
	}

	return
}

func newClient(c *cli.Command) (*kraken.Client, error) {
	cl, err := kraken.New(c.String("key"), c.String("secret"))
	if err != nil {
		return nil, errors.Wrap(err, "new client")
	}

	if q := c.String("user-agent"); q != "" {
		cl.UserAgent = q
	}

	return cl, nil
}

func test(c *cli.Command) (err error) {
	httpcl := http.Client{
		Transport: httpdumper{},
	}

	cl := krakenapi.NewWithClient(c.String("key"), c.String("secret"), &httpcl)

	b, err := cl.Balance()
	if err != nil {
		return errors.Wrap(err, "call")
	}

	return out(c, b)
}

type httpdumper struct {
}

func (d httpdumper) RoundTrip(req *http.Request) (*http.Response, error) {
	req.Header.Set("User-Agent", "some test golang client")

	data, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, errors.Wrap(err, "read body")
	}

	req.Body = io.NopCloser(bytes.NewReader(data))

	w := tlog.DefaultLogger.IOWriter(1)

	err = req.Write(w)
	if err != nil {
		return nil, errors.Wrap(err, "encode request")
	}

	req.Body = io.NopCloser(bytes.NewReader(data))

	return http.DefaultTransport.RoundTrip(req)
}
