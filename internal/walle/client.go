package walle

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/go-resty/resty/v2"
	"golang.org/x/xerrors"
)

const (
	defaultHTTPClientTimeout = time.Second * 30

	walleURL = "https://api.wall-e.yandex-team.ru"
)

var ErrHostNotFound = xerrors.New("host not found")

// Project is a name of the Wall-e project.
type Project string

type ClientOption func(c *Client)

type Client struct {
	httpClient *resty.Client
}

func NewClient(options ...ClientOption) *Client {
	c := &Client{
		httpClient: resty.New().SetTimeout(defaultHTTPClientTimeout),
	}

	for _, o := range options {
		o(c)
	}

	return c
}

func (c *Client) FindProject(ctx context.Context, host string) (Project, error) {
	type project struct {
		Project Project `json:"project"`
	}

	type walleError struct {
		Message string `json:"message"`
	}

	queryURL := fmt.Sprintf("%s/v1/hosts/%s", walleURL, host)
	resp, err := c.httpClient.R().
		SetContext(ctx).
		SetQueryParam("fields", "name,project").
		SetResult(&project{}).
		SetError(&walleError{}).
		Get(queryURL)

	if err != nil {
		return "", err
	}

	if resp.StatusCode() == http.StatusOK {
		return resp.Result().(*project).Project, nil
	}

	if resp.Error() != nil {
		msg := resp.Error().(*walleError).Message
		if msg == "The specified host doesn't exist." {
			return "", ErrHostNotFound
		}
	}

	return "", xerrors.Errorf("unexpected walle response: %s", resp.String())
}

func (c *Client) GetHostInfo(ctx context.Context, hosts ...string) (map[string]*HostInfo, error) {
	type result struct {
		NextCursor int         `json:"next_cursor"`
		Result     []*HostInfo `json:"result"`
	}

	type walleError struct {
		Message string `json:"message"`
	}

	queryURL := fmt.Sprintf("%s/v1/get-hosts", walleURL)
	hostInfo := make(map[string]*HostInfo)

	for cursor := 0; ; {
		resp, err := c.httpClient.R().
			SetContext(ctx).
			SetQueryParam("fields", "name,ticket").
			SetQueryParam("cursor", strconv.Itoa(cursor)).
			SetBody(map[string][]string{
				"names": hosts,
			}).
			SetResult(&result{}).
			SetError(&walleError{}).
			Post(queryURL)

		if err != nil {
			return nil, err
		}

		if resp.Error() != nil {
			msg := resp.Error().(*walleError).Message
			return nil, xerrors.Errorf("unable to get host info: %s", msg)
		}

		if resp.StatusCode() != http.StatusOK {
			return nil, xerrors.Errorf("bad response status code: %d", resp.StatusCode())
		}

		result := resp.Result().(*result)
		for _, i := range result.Result {
			hostInfo[i.Name] = i
		}

		if result.NextCursor == 0 {
			break
		}

		cursor = result.NextCursor
	}

	return hostInfo, nil
}

type HostInfo struct {
	Name   string `json:"name"`
	Ticket string `json:"ticket"`
}
