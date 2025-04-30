package auth

import (
	"fmt"

	"github.com/casbin/casbin"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Authorizer struct {
	enforcer *casbin.Enforcer
}

// New creates a new Authorizer instance with a casbin enforcer
// initialized using the provided model and policy files. It returns
// a pointer to the created Authorizer.
func New(model, policy string) *Authorizer {
	enforcer := casbin.NewEnforcer(model, policy)
	return &Authorizer{
		enforcer: enforcer,
	}
}

// Authorize checks if the given subject has the given permission to
// perform the given action on the given object. It returns an error if
// the subject does not have the specified permission.
func (a *Authorizer) Authorize(subject, object, action string) error {

	if !a.enforcer.Enforce(subject, object, action) {
		msg := fmt.Sprintf(
			"%s not permitted to %s to %s",
			subject,
			action,
			object,
		)

		st := status.New(codes.PermissionDenied, msg)

		return st.Err()
	}

	return nil
}
