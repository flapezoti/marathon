package lib

import (
	"context"

	uuid "github.com/satori/go.uuid"
)

// MarathonInterface defines the interface of marathon client
// to access the API.
type MarathonInterface interface {
	CreateJob(
		ctx context.Context,
		template string,
		payload *CreateJobPayload,
	) (*CreateJobResponse, error)
}

// JSON is a generic json map
type JSON map[string]interface{}

// CreateJobPayload contains the parameters for CreateJob method
type CreateJobPayload struct {
	Localized        bool
	ExpiresAt        int64
	StartsAt         int64
	Context          JSON
	Service          string
	Filters          JSON
	Metadata         JSON
	CSVPath          string
	PastTimeStrategy interface{}
	ControlGroup     float64
}

// CreateJobResponse is the response for CreateJob method
type CreateJobResponse struct {
	ID                  uuid.UUID
	TotalBatches        int
	CompletedBatches    int
	TotalUsers          int
	CompletedUsers      int
	CompletedTokens     int
	DBPageSize          int
	Localized           bool
	CompletedAt         int64
	ExpiresAt           int64
	StartsAt            int64
	Context             JSON
	Service             string
	Filters             JSON
	Metadata            JSON
	CSVPath             string
	TemplateName        string
	PastTimeStrategy    string
	Status              string
	AppID               uuid.UUID
	CreatedBy           string
	CreatedAt           int64
	UpdatedAt           int64
	ControlGroup        float64
	ControlGroupCsvPath string
}
