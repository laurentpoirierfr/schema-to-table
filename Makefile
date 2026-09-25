# Schema -> Table : expérimentation / prototype
#
# Targets:
#   make db-up                  démarrer PostgreSQL local (compose)
#   make db-down                arrêter et purger les volumes
#   make test                   tests unitaires (sans DB)
#   make test-integration       tests E2E contre la DB (compose doit tourner)
#   make test-processors        tests unitaires des processeurs Bento (sans DB)
#   make test-integration-processors
#                               test E2E des processeurs Bento contre la DB
#   make run                    lancer le CLI (SQL affiché sur stdout)
#   make demo                   lancer le CLI et exécuter contre la base (mode plat JSONB)
#   make demo-model             idem en mode normalisé (tout-tabulaire + vues dénormalisées)
#   make image                  construire l'image Bento custom (processeurs compilés)
#   make demo-bento             démo complète : compose (postgres + schémas + Bento)
#   make bento                  lancer le binaire Bento custom local avec la config d'exemple

.PHONY: db-up db-down db-logs test test-integration test-processors test-integration-processors run demo demo-model build vet fmt image demo-bento bento bento-lint

db-up:
	docker compose up -d --wait

db-down:
	docker compose down --volumes

db-logs:
	docker compose logs -f postgres

build:
	go build ./...

vet:
	go vet ./...

fmt:
	gofmt -w .

test:
	go test ./...

test-integration:
	go test -tags integration ./tests -run TestIntegration -v

test-processors:
	go test ./pkg/processors -v

test-integration-processors:
	go test -tags integration ./pkg/processors -run TestProcessorsIntegration -count=1 -v

image:
	docker build -t schema-to-table-bento .

demo-bento: image
	docker compose up -d --wait

bento:
	go build -o /tmp/s2t-bento ./cmd/bento

bento-lint: bento
	/tmp/s2t-bento -c bento/config.yaml lint

run:
	go run ./cmd/proto -schema schemas/order/schema.json -data schemas/order/datas \
		-table landing_order \
		-complex-type JSONB -mode all \
		-headers source=TEXT,ingested_at=TIMESTAMPTZ \
		-header-values source=proto,ingested_at=$(shell date -u +%Y-%m-%dT%H:%M:%SZ)

demo: db-up
	go run ./cmd/proto -schema schemas/order/schema.json -data schemas/order/datas \
		-table landing_order \
		-complex-type JSONB -mode all -drop -pk id \
		-dsn postgres://s2t:s2t@localhost:5432/s2t?sslmode=disable \
		-headers source=TEXT,ingested_at=TIMESTAMPTZ \
		-header-values source=proto,ingested_at=$(shell date -u +%Y-%m-%dT%H:%M:%SZ)

demo-model: db-up
	go run ./cmd/proto -schema schemas/order/schema.json -data schemas/order/datas \
		-table landing_order \
		-mode all -model -pk id -drop \
		-dsn postgres://s2t:s2t@localhost:5432/s2t?sslmode=disable \
		-headers source=TEXT,ingested_at=TIMESTAMPTZ \
		-header-values source=proto-model,ingested_at=$(shell date -u +%Y-%m-%dT%H:%M:%SZ)