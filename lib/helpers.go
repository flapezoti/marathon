package lib

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"time"

	ehttp "github.com/topfreegames/extensions/http"
)

var client *http.Client

func getHTTPClient(config *Config) *http.Client {
	if client == nil {
		client = &http.Client{
			Transport: getHTTPTransport(config),
			Timeout:   config.Timeout,
		}

		ehttp.Instrument(client)
	}

	return client
}

func getHTTPTransport(config *Config) http.RoundTripper {
	if _, ok := http.DefaultTransport.(*http.Transport); !ok {
		return http.DefaultTransport // tests should use a mock transport
	}

	// We can't get http.DefaultTransport here and update its
	// fields since it's an exported variable, so other libs could
	// also change it and overwrite. This hardcoded values are copied
	// from http.DefaultTransport but could be configurable too.
	return &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          config.MaxIdleConns,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		MaxIdleConnsPerHost:   config.MaxIdleConnsPerHost,
	}
}

func (m *Marathon) sendTo(
	ctx context.Context,
	method, url string,
	payload, response interface{},
) error {
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	var req *http.Request

	if payload != nil {
		req, err = http.NewRequest(method, url, bytes.NewBuffer(payloadJSON))
		if err != nil {
			return err
		}
	} else {
		req, err = http.NewRequest(method, url, nil)
		if err != nil {
			return err
		}
	}
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth(m.user, m.pass)
	if ctx == nil {
		ctx = context.Background()
	}
	req = req.WithContext(ctx)

	resp, err := m.httpClient.Do(req)
	if err != nil {
		return err
	}

	defer func() {
		_ = resp.Body.Close()
	}()

	body, respErr := ioutil.ReadAll(resp.Body)
	if respErr != nil {
		return respErr
	}

	if resp.StatusCode > 399 {
		return newRequestError(resp.StatusCode, string(body))
	}

	err = json.Unmarshal(body, response)
	if err != nil {
		return err
	}

	return nil
}

func (m *Marathon) buildURL(pathname string) string {
	return fmt.Sprintf("%s/apps/%s/%s", m.url, m.appID, pathname)
}

func (m *Marathon) buildCreateJobURL(template string) string {
	return m.buildURL(fmt.Sprintf("job?template=%s", template))
}
