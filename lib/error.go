package lib

import "fmt"

// RequestError contains code and body of a request that failed
type RequestError struct {
	statusCode int
	body       string
}

func newRequestError(statusCode int, body string) *RequestError {
	return &RequestError{
		statusCode: statusCode,
		body:       body,
	}
}

func (r *RequestError) Error() string {
	return fmt.Sprintf("Request error. Status code: %d. Body: %s", r.statusCode, r.body)
}

// Status returns the status code of the error
func (r *RequestError) Status() int {
	return r.statusCode
}
