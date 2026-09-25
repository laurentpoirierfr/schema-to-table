// Package main builds the custom Bento distribution of schema-to-table: the
// whole Bento CLI, the common connectors and the schema_to_table_insert /
// schema_to_table_upsert processors compiled in (they register themselves as
// soon as the package is imported). Build it with:
//
//	go build -o bento ./cmd/bento
//
// then run it like the stock Bento binary:
//
//	./bento -c bento/config.yaml
//	./bento create kafka//stdin > bento.yaml
//	./bento -c bento/config.yaml lint
//
// Connector coverage is curated on purpose: public/components/all also pulls
// experimental trees (parquet-go, avro…) whose linkname-free builds break on
// Go 1.25 (runtime.aeskeysched removed). pure + io cover the demo config
// (http_server, drop), kafka the most common production streams.
package main

import (
	"context"

	// base components (stdin/stdout, drop, mapping, brokers…).
	_ "github.com/warpstreamlabs/bento/v4/public/components/pure"
	// network/socket connectors (http_server, files…).
	_ "github.com/warpstreamlabs/bento/v4/public/components/io"
	// Kafka input/output for production pipelines.
	_ "github.com/warpstreamlabs/bento/v4/public/components/kafka"
	// Prometheus exporter for the /metrics endpoint of the supervision API.
	_ "github.com/warpstreamlabs/bento/v4/public/components/prometheus"
	// register the schema-to-table processors.
	_ "github.com/laurentpoirierfr/schema-to-table/pkg/processors"

	"github.com/warpstreamlabs/bento/v4/public/service"
)

func main() {
	service.RunCLI(context.Background())
}
