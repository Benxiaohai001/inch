package cnosdb

import (
	"bytes"
	"crypto/tls"
	"errors"
	"fmt"
	client "github.com/influxdata/influxdb1-client/v2"
	"io"
	"net/http"
	"net/url"
	"path"
	"time"
)

var _ client.Client = &Client{}

func NewClient(conf client.HTTPConfig) (*Client, error) {
	u, err := url.Parse(conf.Addr)
	if err != nil {
		return nil, err
	} else if u.Scheme != "http" && u.Scheme != "https" {
		m := fmt.Sprintf("Unsupported protocol scheme: %s, your address"+
			" must start with http:// or https://", u.Scheme)
		return nil, errors.New(m)
	}

	tr := &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: conf.InsecureSkipVerify,
		},
		Proxy: conf.Proxy,
	}
	if conf.TLSConfig != nil {
		tr.TLSClientConfig = conf.TLSConfig
	}

	return &Client{
		url:      *u,
		username: conf.Username,
		password: conf.Password,
		httpClient: &http.Client{
			Timeout:   conf.Timeout,
			Transport: tr,
		},
		transport: tr,
	}, nil
}

type Client struct {
	url        url.URL
	username   string
	password   string
	httpClient *http.Client
	transport  *http.Transport
}

type PingResponse struct {
	Version string `json:"version"`
	Status  string `json:"status"`
}

func (c *Client) Ping(_timeout time.Duration) (time.Duration, string, error) {
	panic("implement me")
}

func (c *Client) Write(bp client.BatchPoints) error {
	var b bytes.Buffer

	for _, p := range bp.Points() {
		if p == nil {
			continue
		}
		if _, err := b.WriteString(p.PrecisionString(bp.Precision())); err != nil {
			return err
		}

		if err := b.WriteByte('\n'); err != nil {
			return err
		}
	}

	u := c.url
	u.Path = path.Join(u.Path, "api/v1/write")

	req, err := http.NewRequest("POST", u.String(), &b)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "")
	if c.username != "" {
		req.SetBasicAuth(c.username, c.password)
	}

	params := req.URL.Query()
	params.Set("db", bp.Database())
	params.Set("precision", bp.Precision())
	req.URL.RawQuery = params.Encode()

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		var err = errors.New(string(body))
		return err
	}

	return nil
}

func (c *Client) Query(q client.Query) (*client.Response, error) {
	u := c.url
	u.Path = path.Join(u.Path, "api/v1/sql")

	req, err := http.NewRequest("POST", u.String(), bytes.NewReader([]byte(q.Command)))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "")
	if c.username != "" {
		req.SetBasicAuth(c.username, c.password)
	}

	params := req.URL.Query()
	params.Set("db", q.Database)
	req.URL.RawQuery = params.Encode()

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		var err = errors.New(string(body))
		return nil, err
	}

	return &client.Response{
		Results: []client.Result{},
		Err:     "",
	}, nil
}

func (c *Client) QueryAsChunk(q client.Query) (*client.ChunkedResponse, error) {
	panic("implement me")
}

func (c *Client) Close() error {
	c.transport.CloseIdleConnections()
	return nil
}
