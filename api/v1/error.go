package log_v1

import (
	"fmt"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	status "google.golang.org/grpc/status"
)

type ErrorOffsetOutOfRange struct {
	Offset uint64
}

// GRPCStatus returns a grpc.Status that represents the error. The status is
// an InvalidArgument error with a description that includes the given offset.
func (e ErrorOffsetOutOfRange) GRPCStatus() *status.Status {
	st := status.New(
		404,
		fmt.Sprintf("offset %d out of range", e.Offset),
	)

	msg := fmt.Sprintf(
		"The requested offset is outside the log's range:%d",
		e.Offset,
	)

	d := &errdetails.LocalizedMessage{
		Locale:  "en-US",
		Message: msg,
	}
	std, err := st.WithDetails(d)
	if err != nil {
		return st
	}

	return std
}

// Error implements the error interface. It returns the result of calling
// GRPCStatus().Err().Error().
func (e ErrorOffsetOutOfRange) Error() string {
	return e.GRPCStatus().Err().Error()
}
