// Package rpc 提供以太坊 JSON-RPC 客户端 Client（Dial、Call），支持读写分离（广播走 BroadcastURL）。
package rpc

import (
	"context"
	"errors"
	"fmt"

	goethrpc "github.com/ethereum/go-ethereum/rpc"
	"github.com/imroc/req"
	"github.com/tidwall/gjson"
)

// Client 以太坊 JSON-RPC 客户端（支持读写分离：广播走 BroadcastURL）
type Client struct {
	ctx          context.Context
	BaseURL      string
	BroadcastURL string
	RawClient    *goethrpc.Client
}

// Dial 连接节点
func Dial(baseURL, broadcastURL string) (*Client, error) {
	ctx := context.Background()
	client := &Client{ctx: ctx, BaseURL: baseURL, BroadcastURL: broadcastURL}
	rawClient, err := goethrpc.DialContext(ctx, baseURL)
	if err != nil {
		return nil, err
	}
	client.RawClient = rawClient
	return client, nil
}

// Call 调用 RPC；eth_sendRawTransaction 时使用 BroadcastURL
func (c *Client) Call(method string, params []interface{}) (*gjson.Result, error) {
	if method == "eth_sendRawTransaction" && c.BroadcastURL != "" {
		return c.callHTTP(c.BroadcastURL, method, params)
	}
	return c.callHTTP(c.BaseURL, method, params)
}

func (c *Client) callHTTP(url, method string, params []interface{}) (*gjson.Result, error) {
	body := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  method,
		"params":  params,
	}
	r, err := req.Post(url, req.BodyJSON(&body), req.Header{"Content-Type": "application/json", "Accept": "application/json"})
	if err != nil {
		return nil, err
	}
	resp := gjson.ParseBytes(r.Bytes())
	if err := checkError(&resp); err != nil {
		return nil, err
	}
	result := resp.Get("result")
	return &result, nil
}

func checkError(resp *gjson.Result) error {
	if resp.Get("error").IsObject() {
		return errors.New(resp.Get("error.message").String())
	}
	if !resp.Get("result").Exists() && resp.Get("error").Type == gjson.Null {
		return fmt.Errorf("response empty")
	}
	return nil
}

// CallRaw 使用原生 rpc 客户端调用（可选）
func (c *Client) CallRaw(method string, params ...interface{}) (interface{}, error) {
	var res interface{}
	err := c.RawClient.CallContext(c.ctx, &res, method, params...)
	if err != nil {
		return nil, err
	}
	return res, nil
}

// Close 关闭连接
func (c *Client) Close() {
	if c.RawClient != nil {
		c.RawClient.Close()
	}
}
