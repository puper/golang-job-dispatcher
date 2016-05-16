package jsonrpc

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"
)

type Client struct {
	Url string
	id  int
	c   *http.Client
}

type Response struct {
	Id     int            `json:"id"`
	Result interface{}    `json:"result"`
	Error  *ResponseError `json:"error"`
}

type ResponseError struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data"`
}

func (this *ResponseError) Error() string {
	b, err := json.Marshal(this)
	if err != nil {
		return err.Error()
	}
	return string(b)
}

func NewClient(url string) *Client {
	client := new(Client)
	client.Url = url
	client.c = &http.Client{}
	return client
}

func (c *Client) Call(method string, params interface{}) (*Response, error) {
	return c.CallTimeout(method, params, 0)
}

func (c *Client) CallTimeout(method string, params interface{}, timeout time.Duration) (*Response, error) {
	var payload = map[string]interface{}{
		"jsonrpc": "2.0",
		"method":  method,
		"params":  params,
		"id":      c.id,
	}

	c.id += 1
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	buf := bytes.NewBuffer(data)
	c.c.Timeout = timeout
	resp, err := c.c.Post(c.Url, "application/json", buf)

	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	decoder := json.NewDecoder(resp.Body)
	var respPayload *Response
	err = decoder.Decode(&respPayload)

	if err != nil {
		return nil, errors.New(fmt.Sprintf("Invalid response from server. Status: %s. %s", resp.Status, err.Error()))
	}

	return respPayload, nil
}
