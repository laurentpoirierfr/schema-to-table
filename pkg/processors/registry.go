package processors

import "github.com/warpstreamlabs/bento/v4/public/service"

// RegisterBoth registers the two Bento processors into the environment.
// Importing this package also registers them via init.
func RegisterBoth() error {
	if err := service.RegisterBatchProcessor(insertProcessorName, insertSpec(), newInsertProcessor); err != nil {
		return err
	}
	return service.RegisterBatchProcessor(upsertProcessorName, upsertSpec(), newUpsertProcessor)
}

// registrationErr is the outcome of init-time registration, surfaced to
// unit tests.
var registrationErr error

func init() {
	registrationErr = RegisterBoth()
}
