package rest

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

const requestTimeout = 300 * time.Second

// Client - request client for any REST API
type Client struct {
	address    string
	username   string
	password   string
	httpClient *http.Client
	log        *logrus.Entry

	mux       sync.Mutex
	requestID int64
}

// ClientInterface - request client interface
type ClientInterface interface {
	BuildURI(uri string, params map[string]string) string
	Send(path string, data interface{}) (int, []byte, error)
}

// BuildURI builds request URI using [path?params...] format
func (c *Client) BuildURI(uri string, params map[string]string) string {
	paramsStr := ""
	paramValues := url.Values{}

	for key, val := range params {
		if len(val) != 0 {
			paramValues.Set(key, val)
		}
	}

	paramsStr = paramValues.Encode()
	if len(paramsStr) != 0 {
		uri = fmt.Sprintf("%s?%s", uri, paramsStr)
	}

	return uri
}

// Send sends request to REST server
// data interface{} - request payload, any interface for json.Marshal()
func (c *Client) Send(path string, data interface{}) (int, []byte, error) {
	c.mux.Lock()
	c.requestID++
	l := c.log.WithFields(logrus.Fields{
		"func":  "Send()",
		"req":   path,
		"reqID": c.requestID,
	})
	c.mux.Unlock()

	uri := fmt.Sprintf("%s/zebi/api/v2/%s", c.address, path)
	l.Debugf("url: %+v", uri)

	// send request data as json
	var jsonDataReader io.Reader
	if data != nil {
		jsonData, err := json.Marshal(data)
		if err != nil {
			return 0, nil, err
		}
		jsonDataReader = strings.NewReader(string(jsonData))
		l.Debugf("json: %+v", string(jsonData))
	}

	req, err := http.NewRequest(http.MethodPost, uri, jsonDataReader)
	if err != nil {
		l.Errorf("request creation error: %s", err)
		return 0, nil, err
	}

	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth(c.username, c.password)

	res, err := c.httpClient.Do(req)
	if err != nil {
		l.Debugf("request error: %s", err)
		return 0, nil, err
	}

	defer res.Body.Close()

	l.Debugf("response status code: %d", res.StatusCode)

	// validate response body
	bodyBytes, err := ioutil.ReadAll(res.Body)
	if err != nil {
		err = fmt.Errorf("Cannot read body of request '%s': '%s'", uri, err)
		return res.StatusCode, nil, err
	}

	l.Debugf("response body: %s", bodyBytes)

	return res.StatusCode, bodyBytes, err
}

// ClientArgs - params to create Client instance
type ClientArgs struct {
	Address  string
	Username string
	Password string
	Log      *logrus.Entry

	// InsecureSkipVerify controls whether a client verifies the server's certificate chain and host name.
	InsecureSkipVerify bool
}

// NewClient creates new REST client
func NewClient(args ClientArgs) ClientInterface {
	l := args.Log.WithField("cmp", "RestClient")

	tr := &http.Transport{
		IdleConnTimeout: 60 * time.Second,
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: args.InsecureSkipVerify,
		},
	}

	httpClient := &http.Client{
		Transport: tr,
		Timeout:   requestTimeout,
	}

	l.Debugf("created for '%s'", args.Address)
	return &Client{
		address:    args.Address,
		username:   args.Username,
		password:   args.Password,
		httpClient: httpClient,
		log:        l,
		requestID:  0,
	}
}
