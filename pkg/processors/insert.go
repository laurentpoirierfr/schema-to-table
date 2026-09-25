package processors

import "github.com/warpstreamlabs/bento/v4/public/service"

// newInsertProcessor is the Bento constructor for the insert processor.
func newInsertProcessor(conf *service.ParsedConfig, _ *service.Resources) (service.BatchProcessor, error) {
	cfg, err := parseConfig(conf)
	if err != nil {
		return nil, err
	}
	return &batchProcessor{cfg: cfg, loader: newSchemaLoader(cfg.schemaTTL)}, nil
}
