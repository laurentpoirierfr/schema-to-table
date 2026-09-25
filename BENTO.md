# Bento : la distribution `schema_to_table`

Les processeurs [`schema_to_table_insert` / `schema_to_table_upsert`](PROCESSORS.md) sont des plugins Bento **compilés en Go** : ils ne peuvent pas être chargés à chaud par [l'image Bento officielle](ghcr.io/warpstreamlabs/bento). Pour les utiliser en production, ce dépôt embarque donc un **binaire Bento custom** dans une **image Docker**, accompagné d'une config d'exemple et d'un `compose.yaml` de démonstration.

- **Référence de configuration des processeurs** → [PROCESSORS.md](PROCESSORS.md) (champs, `on_error`, dead-letter queue, architecture interne).
- **Distribution (ce document)** : binaire, image, config d'exemple, démo, production.

## Le binaire `cmd/bento`

`cmd/bento/main.go` est le CLI Bento complet (`-c`, `lint`, `test`, `create`, `list`…) auquel sont greffés les processeurs du dépôt :

```go
_ "github.com/warpstreamlabs/bento/v4/public/components/pure"        // base (drop, brokers…)
_ "github.com/warpstreamlabs/bento/v4/public/components/io"          // http_server, fichiers… (stdlib)
_ "github.com/warpstreamlabs/bento/v4/public/components/kafka"       // flux de prod courants
_ "github.com/warpstreamlabs/bento/v4/public/components/prometheus"  // /metrics (Prometheus)
_ "github.com/laurentpoirierfr/schema-to-table/pkg/processors"       // nos processeurs
service.RunCLI(context.Background())
```

> Le bundle `public/components/all` est volontairement **évité** : il tire des arbres expérimentaux (`parquet-go`, avro…) qui ne linkent plus sous Go 1.25 (référence par linkname à `runtime.aeskeysched`, supprimé du runtime). Notre sous-ensemble couvre la config d'exemple et les usages de streaming typiques.

### Construire et lancer

```sh
go build -o bento ./cmd/bento     # make bento (vers /tmp/s2t-bento)
./bento -c bento/config.yaml      # lancement
./bento -c bento/config.yaml lint # validation statique de la config
./bento list processors | grep schema_to_table
```

## L'image Docker

`Dockerfile` multi-étapes :

| Étape | Image | Rôle |
|-------|-------|------|
| build | `golang:1.25-alpine` | `CGO_ENABLED=0 go build ./cmd/bento` (pgx est pur Go, pas de cgo) |
| certs | `debian:bookworm-slim` | extrait les CA racines (`/etc/ssl/certs/ca-certificates.crt`) |
| run | `distroless/static-debian12:nonroot` | `/bento`, utilisateur non-root — aucun shell, aucun package |

- `/config` est déclaré `VOLUME` : la config est montée en lecture seule (ex. `-v ./bento/config.yaml:/config/bento.yaml:ro`).
- `EXPOSE 4195` ; `ENTRYPOINT ["/bento"]`, `CMD ["-c", "/config/bento.yaml"]`.
- Les CA racines sont indispensables pour les `schema_url` en `https://` (le loader les valide sans renier les certificats).

```sh
docker build -t schema-to-table-bento .   # make image
```

## La config d'exemple `bento/config.yaml`

Le pipeline de démonstration : `http_server` → `schema_to_table_insert` → `drop`.

| Élément | Valeur | Pourquoi |
|---------|--------|----------|
| input `http_server` | `0.0.0.0:4195`, `path: /ingest`, `allowed_verbs: [POST]` | les **headers HTTP deviennent les métadonnées** du message → `schema_url`, `table_name`, `source` |
| `http:` (API Bento) | `0.0.0.0:4196` | supervision sur un port distinct de l'input : `/ping`, `/ready`, `/version`, `/metrics` |
| `metrics: prometheus` | `{}` | sans cette section, `/metrics` répond 404 |
| processeur | `schema_to_table_insert` | DSN interpolé `${POSTGRES_DSN:postgres://s2t:s2t@localhost:5432/s2t?sslmode=disable}`, `schema_ttl: 1h`, `on_error: per_message` |
| output `drop` | `{}` | les lignes sont déjà écrites **dans la transaction du processeur** ; la sortie n'a plus qu'à acquitter — imaginer ici un output SQL serait un doublon |

## Quick start (compose + curl)

```sh
make demo-bento            # = docker compose up : postgres + schema-server + bento
```

`schema-server` (python `http.server`) sert la **racine du dépôt** en lecture seule : le chemin `schemas/<sujet>/schema.json` de l'URL reflète le layout du repo.

```sh
curl -X POST http://localhost:4195/ingest \
  -H "schema_url: http://schema-server:8080/schemas/employee/schema.json" \
  -H "table_name: landing_employee" \
  -H "source: curl-demo" \
  -d '{"id":"7a0e8400-e29b-41d4-a716-446655440101","kind":"standard","name":"Claire Dubois","department":"Engineering","hourlyRate":42.5,"tags":["golang"]}'
```

Vérifications :

```sh
curl http://localhost:4196/metrics | grep schema_to_table_insert_   # compteurs
docker exec schema-to-table-pg psql -U s2t -d s2t -c '\dt landing_employee*'
```

## Production

- **Fiabilité** : chaque message passe dans une transaction unique (une erreur SQL fait rollback, aucune ligne partielle) ; `on_error: per_message` + la sortie `switch` sur `errored()` donnent la **dead-letter queue**. Détails et exemple → [PROCESSORS.md](PROCESSORS.md#dead-letter-queue-on_error-per_message).
- **Casse des métadonnées** : la lecture est insensible à la casse (Go canonicalise les headers HTTP en `Schema_Url`, Kafka peut porter `SCHEMA_URL`) — voir [PROCESSORS.md](PROCESSORS.md). En HTTP, préférer les headers `schema_url` / `table_name` / `source` tels quels.
- **Observabilité** : compteurs `schema_to_table_insert_*` exposés sur `http://localhost:4196/metrics` (`batches_processed`, `batches_failed`, `messages_processed`, `messages_permanent_errors`, `schema_fetches`, `schema_cache_hits`).
- **Config par environnement** : la DSN se surcharge par `POSTGRES_DSN` (compose cible `postgres://s2t:s2t@postgres:5432/s2t?sslmode=disable`) ; le cache des schémas se règle via `schema_ttl` (`0` désactive).
- **CI** : le job `bento-image` de `.github/workflows/ci.yml` reconstruit l'image et lint la config d'exemple **dans** l'image — un dérive entre `bento/config.yaml` et le binaire est détectée au push/PR.

## Activités Bento jamais supportées ici

Les processeurs étant des plugins **batch** orientés stockage, ne pas :

- brancher un input/output SQL PostgreSQL (le rôle est déjà tenu par le processeur) ;
- s'attendre à un rechargement de plugins à chaud (compilez puis poussez un tag d'image).