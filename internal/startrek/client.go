package startrek

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/go-resty/resty/v2"
	"golang.org/x/xerrors"
)

const (
	defaultHTTPClientTimeout = time.Second * 30
)

var (
	ErrTransitionUnavailable = xerrors.New("transition unavailable")
	ErrAlreadyRelated        = xerrors.New("already related")
)

type ClientConfig struct {
	OAuthToken string `yaml:"-"`

	URL string `yaml:"api_url"`
}

type ClientOption func(c *Client)

func WithCustomHTTPClient(httpClient *resty.Client) ClientOption {
	return func(c *Client) {
		c.httpClient = httpClient
	}
}

type Client struct {
	conf *ClientConfig

	httpClient *resty.Client
}

func NewClient(conf *ClientConfig, options ...ClientOption) *Client {
	c := &Client{
		conf:       conf,
		httpClient: resty.New().SetTimeout(defaultHTTPClientTimeout),
	}

	for _, o := range options {
		o(c)
	}

	return c
}

func (c *Client) CreateTicket(ctx context.Context, t *Ticket) (TicketKey, error) {
	queryURL := fmt.Sprintf("%s/issues", c.conf.URL)

	resp, err := c.httpClient.R().
		SetContext(ctx).
		SetHeaders(map[string]string{
			"Content-Type":  "application/json",
			"Authorization": fmt.Sprintf("OAuth %s", c.conf.OAuthToken),
		}).
		SetBody(t).
		SetResult(&NewTicketResponse{}).
		Post(queryURL)

	if err != nil {
		return "", xerrors.Errorf("failed startrek query: %w", err)
	}

	if resp.StatusCode() != http.StatusCreated {
		return "", xerrors.Errorf("bad response status %d, body: %s", resp.StatusCode(), resp.String())
	}

	return resp.Result().(*NewTicketResponse).Key, nil
}

func (c *Client) UpdateTicket(ctx context.Context, k TicketKey, t *Ticket) (*Ticket, error) {
	queryURL := fmt.Sprintf("%s/issues/%s", c.conf.URL, k)

	resp, err := c.httpClient.R().
		SetContext(ctx).
		SetHeaders(map[string]string{
			"Content-Type":  "application/json",
			"Authorization": fmt.Sprintf("OAuth %s", c.conf.OAuthToken),
		}).
		SetBody(t).
		SetResult(&Ticket{}).
		Patch(queryURL)

	if err != nil {
		return nil, xerrors.Errorf("failed startrek query: %w", err)
	}

	if resp.StatusCode() != http.StatusOK {
		return nil, xerrors.Errorf("bad response status %d, body: %s", resp.StatusCode(), resp.String())
	}

	return resp.Result().(*Ticket), nil
}

func (c *Client) StartProgress(ctx context.Context, key TicketKey, assignee string) error {
	_, _ = c.UpdateTicket(ctx, key, &Ticket{Assignee: &Entity{Login: assignee}})
	return c.EditStatus(ctx, key, TransitionStartProgress, nil)
}

// LinkTickets creates relationship between source and target tickets.
//
// Returns error wrapping ErrAlreadyRelated if given issues already have given relation.
func (c *Client) LinkTicket(ctx context.Context, source, target TicketKey, r TicketRelationship) error {
	queryURL := fmt.Sprintf("%s/issues/%s/links", c.conf.URL, source)

	type startrekError struct {
		Errors        any      `json:"errors"`
		ErrorMessages []string `json:"errorMessages"`
		StatusCode    int      `json:"statusCode"`
	}

	resp, err := c.httpClient.R().
		SetContext(ctx).
		SetHeaders(map[string]string{
			"Content-Type":  "application/json",
			"Authorization": fmt.Sprintf("OAuth %s", c.conf.OAuthToken),
		}).
		SetBody(map[string]any{
			"relationship": r,
			"issue":        target,
		}).
		SetError(&startrekError{}).
		Post(queryURL)

	if err != nil {
		return xerrors.Errorf("failed startrek query: %w", err)
	}

	switch resp.StatusCode() {
	case http.StatusCreated:
		return nil
	case http.StatusUnprocessableEntity:
		err := resp.Error().(*startrekError)
		msg := "already related"
		if len(err.ErrorMessages) > 0 {
			msg = err.ErrorMessages[0]
		}
		return xerrors.Errorf("%s status code %d: %w", msg, err.StatusCode, ErrAlreadyRelated)
	default:
		return xerrors.Errorf("bad response status %d, body: %s", resp.StatusCode(), resp.String())
	}
}

func (c *Client) CloseTicket(ctx context.Context, key TicketKey) error {
	return c.EditStatus(ctx, key, TransitionClose,
		map[string]string{
			"resolution": "fixed",
			"comment":    "another one",
		},
	)
}

func (c *Client) EditStatus(ctx context.Context, ticket TicketKey, transition Transition, payload any) error {
	ok, err := c.checkTransitionAvailable(ctx, ticket, transition)
	if err != nil {
		return err
	}

	if !ok {
		return ErrTransitionUnavailable
	}

	return c.makeTransition(ctx, ticket, transition, payload)
}

func (c *Client) checkTransitionAvailable(ctx context.Context, ticket TicketKey, transition Transition) (bool, error) {
	queryURL := fmt.Sprintf("%s/issues/%s/transitions", c.conf.URL, ticket)

	var transitions []struct {
		ID Transition `json:"id"`
	}

	resp, err := c.httpClient.R().
		SetContext(ctx).
		SetHeaders(map[string]string{
			"Content-Type":  "application/json",
			"Authorization": fmt.Sprintf("OAuth %s", c.conf.OAuthToken),
		}).
		SetResult(&transitions).
		Get(queryURL)

	if err != nil {
		return false, xerrors.Errorf("failed startrek query: %w", err)
	}

	if resp.StatusCode() != http.StatusOK {
		return false, xerrors.Errorf("bad response status %d, body: %s", resp.StatusCode(), resp.String())
	}

	for _, t := range transitions {
		if t.ID == transition {
			return true, nil
		}
	}

	return false, nil
}

func (c *Client) makeTransition(ctx context.Context, ticket TicketKey, transition Transition, payload any) error {
	queryURL := fmt.Sprintf("%s/issues/%s/transitions/%s/_execute", c.conf.URL, ticket, transition)

	resp, err := c.httpClient.R().
		SetContext(ctx).
		SetHeaders(map[string]string{
			"Content-Type":  "application/json",
			"Authorization": fmt.Sprintf("OAuth %s", c.conf.OAuthToken),
		}).
		SetBody(payload).
		Post(queryURL)

	if err != nil {
		return xerrors.Errorf("failed startrek query: %w", err)
	}

	if resp.StatusCode() != http.StatusOK {
		return xerrors.Errorf("bad response status %d, body: %s", resp.StatusCode(), resp.String())
	}

	return nil
}
