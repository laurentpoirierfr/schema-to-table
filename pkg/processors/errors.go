package processors

import "errors"

// permanentError marks a message-scoped failure that no retry can fix: a
// missing metadata header, an unreachable or invalid schema document, or a
// payload that does not fit the planned model. Under on_error: per_message
// such failures reject the offending message instead of aborting the batch.
type permanentError struct {
	err error
}

func (e *permanentError) Error() string { return e.err.Error() }
func (e *permanentError) Unwrap() error { return e.err }

// permanent wraps err as a permanent (message-scoped) failure.
func permanent(err error) error {
	if err == nil {
		return nil
	}
	return &permanentError{err: err}
}

// isPermanent reports whether the failure is message-scoped and thus unfit
// for retries. Anything else (connectivity, HTTP 5xx, transaction or SQL
// errors) is treated as a recoverable, batch-level error.
func isPermanent(err error) bool {
	var p *permanentError
	return errors.As(err, &p)
}
