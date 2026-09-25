package processors

import "github.com/warpstreamlabs/bento/v4/public/service"

// newUpsertProcessor is the Bento constructor for the upsert processor.
func newUpsertProcessor(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
	cfg, err := parseConfig(conf)
	if err != nil {
		return nil, err
	}
	p := &batchProcessor{cfg: cfg, upsert: true, loader: newSchemaLoader(cfg.schemaTTL)}
	p.wire(upsertProcessorName, mgr)
	return p, nil
}
