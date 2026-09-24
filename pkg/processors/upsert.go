package processors

import "github.com/warpstreamlabs/bento/v4/public/service"

// newUpsertProcessor is the Bento constructor for the upsert processor.
func newUpsertProcessor(conf *service.ParsedConfig, _ *service.Resources) (service.BatchProcessor, error) {
	cfg, err := parseConfig(conf)
	if err != nil {
		return nil, err
	}
	return &batchProcessor{cfg: cfg, upsert: true, loader: newSchemaLoader()}, nil
}
