package util

import (
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"time"
)

// HTTPAlive implements a simple way of reusing http connections
type HTTPAlive struct {
	client    *http.Client
	transport *http.Transport
}

// HTTPAliveResponse returns a response
type HTTPAliveResponse struct {
	StatusCode int
	Body       []byte
	Header     http.Header
}

// Configure the http connection
func (connection *HTTPAlive) Configure(timeout time.Duration,
	aliveDuration time.Duration,
	maxIdleConnections int) {
	if connection.transport == nil {
		connection.transport = &http.Transport{
			Dial: (&net.Dialer{
				Timeout:   timeout,
				KeepAlive: aliveDuration,
			}).Dial,
			MaxIdleConnsPerHost: maxIdleConnections,
		}
	}

	if connection.client == nil {
		connection.client = &http.Client{
			Transport: connection.transport,
		}
	}
}

// MakeRequest make a new http request
func (connection *HTTPAlive) MakeRequest(method string,
	uri string, body io.Reader, header map[string]string) (*HTTPAliveResponse, error) {
	req, err := http.NewRequest(method, uri, body)

	if err != nil {
		return nil, err
	}

	// Apply user provided headers
	for key, value := range header {
		req.Header.Set(key, value)
	}

	return connection.submitRequest(req)
}

func (connection *HTTPAlive) submitRequest(req *http.Request) (*HTTPAliveResponse, error) {
	rsp, err := connection.client.Do(req)

	if rsp != nil {
		defer discardResponseBody(rsp.Body)
	}

	if err != nil {
		return nil, err
	}

	body, err := ioutil.ReadAll(rsp.Body)
	if err != nil {
		return nil, err
	}

	httpAliveResponse := new(HTTPAliveResponse)
	httpAliveResponse.Body = body
	httpAliveResponse.StatusCode = rsp.StatusCode
	httpAliveResponse.Header = rsp.Header
	return httpAliveResponse, nil
}

func discardResponseBody(body io.ReadCloser) {
	io.Copy(ioutil.Discard, body)
	body.Close()
}
