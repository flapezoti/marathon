package lib

import (
	"context"
	"net/http"
	"time"
)

// Marathon is the marathon client.
// Implements MarathonInterface.
type Marathon struct {
	httpClient *http.Client
	url        string
	user       string
	pass       string
	appID      string
}

// Config is the configuration struct
// for Marathon http client.
type Config struct {
	Timeout             time.Duration
	MaxIdleConnsPerHost int
	MaxIdleConns        int
	URL                 string
	User                string
	Pass                string
	AppID               string
}

// NewMarathon returns a new Marathon lib
func NewMarathon(config *Config) *Marathon {
	if config.Timeout.Seconds() == 0 {
		config.Timeout = 500 * time.Millisecond
	}

	if config.MaxIdleConnsPerHost == 0 {
		config.MaxIdleConnsPerHost = http.DefaultMaxIdleConnsPerHost
	}

	if config.MaxIdleConns == 0 {
		config.MaxIdleConns = 100
	}

	m := &Marathon{
		httpClient: getHTTPClient(config),
		url:        config.URL,
		user:       config.User,
		pass:       config.Pass,
		appID:      config.AppID,
	}

	return m
}

// CreateJob access Marathon API to create a job
// using template and payload.
func (m *Marathon) CreateJob(
	ctx context.Context,
	template string,
	payload *CreateJobPayload,
) (*CreateJobResponse, error) {
	route := m.buildCreateJobURL(template)
	response := &CreateJobResponse{}

	err := m.sendTo(ctx, "POST", route, payload, response)
	if err != nil {
		return nil, err
	}

	return response, nil
}
