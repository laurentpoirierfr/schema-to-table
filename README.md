# Schema → Table

Générateur de landing tables PostgreSQL à partir d'un [JSON Schema](https://json-schema.org/draft/2020-12/schema) polymorphe et de documents JSON. Le besoin d'origine : remplacer les colonnes `JSONB` génériques par des colonnes **typées** et navigables par SQL, sans réseau ni orchestration — le CLI produit du SQL que vous exécutez où vous voulez, ou l'exécute directement contre une base.

Deux stratégies de stockage sont supportées, au choix :

| Mode | Principe | Usage |
|------|----------|-------|
| **Plat (JSONB)** | une table à plat, les objets complexes/tableaux sérialisés dans une colonne `complexType` | `-model` absent |
| **Normalisé** | une table typée par niveau de tableau + vues dénormalisées `racine × enfant` | `-model` |

Le même moteur est disponible sous forme de **processeurs Bento** (`schema_to_table_insert` / `schema_to_table_upsert`) pour intégrer le stockage dans des pipelines de streaming — voir **[PROCESSORS.md](PROCESSORS.md)**.

## Fonctionnalités

- **Dérivation de colonnes typées** depuis le schéma : `format: uuid` → `UUID`, `integer` → `BIGINT`, `number` → `NUMERIC`, `boolean` → `BOOLEAN`, chaînes → `TEXT`, `date-time` → `TIMESTAMPTZ`.
- **Aplatissement des objets imbriqués 1↔1** en colonnes préfixées (`customer.name`, `address.postalCode` → `customer_name`, `address_postal_code`).
- **Polymorphisme `oneOf`/`allOf`** → modèle par **single-table inheritance** : une table racine, un **discriminateur auto-détecté** (`const` présent dans toutes les variantes, ex. `type` ou `kind`), colonnes des variantes fragmentées dans la table racine (NULL si non applicable).
- **Tableaux** → tables enfants reliées par FK (`<parent>_<clé>`, ex. `landing_order_id`) + `row_no` préservant l'ordre.
- **Arrays de scalaires** → table enfant `(fk, row_no, value)`.
- **Mode normalisé + vues** : une vue dénormalisée par enfant direct (`v_<racine>_<chemin>`), agrégé `SELECT * FROM v_landing_order_packages` sans jointure.
- **Registre des objets** : table `<racine>_registry` documentant chaque table/vue — nom logique, **nom SQL final**, chemin JSON, description (title/description du schéma).
- **Gestion de la limite 63 caractères** : les identifiants longs sont tronqués (préfixe lisible + hash déterministe) de façon cohérente entre DDL, DML et registre.
- **INSERT classique et UPSERT** `ON CONFLICT` (remplacement de la scène complète : DELETE descendants deepest-first puis ré-insertion — les tableaux sans clé naturelle sont remplacés).
- **Colonnes en-tête d'ingestion** (`source`, `ingested_at`, …) typées et insérées à vos côtés.
- **Erreurs explicites** : aucun fallback silencieux — un objet sans propriétés déclarées, un tableau sans `items`, ou une PK absente du payload sont **refusés avec le chemin JSON précis**.

## Installation

```sh
make db-up            # PostgreSQL local (compose, voir compose.yaml)
go build ./...
```

## Utilisation

### Affichage du SQL (aucune base requise)

```sh
# Mode plat (JSONB)
go run ./cmd -schema schemas/order/schema.json -data schemas/order/datas \
    -table landing_order -mode all \
    -headers source=TEXT,ingested_at=TIMESTAMPTZ \
    -header-values source=proto,ingested_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)

# Mode normalisé : tables toutes typées + vues + registre
go run ./cmd -schema schemas/order/schema.json -data schemas/order/datas \
    -table landing_order -mode all -model -pk id \
    -headers source=TEXT,ingested_at=TIMESTAMPTZ \
    -header-values source=proto,ingested_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
```

### Exécution contre PostgreSQL

```sh
go run ./cmd -schema schemas/order/schema.json -data schemas/order/datas \
    -table landing_order -mode all -model -pk id -drop \
    -dsn postgres://s2t:s2t@localhost:5432/s2t?sslmode=disable \
    -headers source=TEXT,ingested_at=TIMESTAMPTZ \
    -header-values source=proto,ingested_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
```

### Options

| Option | Défaut | Description |
|--------|--------|-------------|
| `-schema` | `schemas/order/schema.json` | JSON Schema (draft 2020-12) |
| `-data` | `schemas/order/datas` | répertoire des documents JSON (`*.json`) |
| `-table` | `landing_order` | nom de la table racine |
| `-mode` | `all` | `create` · `insert` · `upsert` · `all` |
| `-model` | `false` | mode normalisé (tables typées + vues + registre) |
| `-pk` | — | colonne(s) de PK (requis pour upsert ; `id` par défaut en mode model) |
| `-conflict` | `id` | colonne de conflit pour l'upsert |
| `-headers` | — | types des colonnes d'ingestion `source=TEXT,ingested_at=TIMESTAMPTZ` |
| `-header-values` | — | valeurs d'ingestion pour le DML |
| `-complex-type` | `JSONB` | colonne des objets/tableaux en mode plat |
| `-dsn` | — | si renseigné, exécute les statements au lieu de les afficher |
| `-drop` | `false` | `DROP` des tables + vues + registre avant création (avec `-dsn`) |

## Exemples

### Schéma polymorphe `order` (discriminateur `type`)

`schemas/order/schema.json` — `oneOf: [standardOrder, expressOrder]`. La table `landing_order` reçoit une colonne `type`, `estimated_days` (variante standard) et `courier_phone` (variante express) sont NULL selon le document. `packages` devient la table `landing_order_packages`, reliée par FOREIGN KEY vers `landing_order(id)`, avec vue `v_landing_order_packages`.

### Schéma polymorphe `employee` (discriminateur `kind`)

`schemas/employee/schema.json` — prouve la généricité avec un **discriminateur différent** du cas précédent (`kind` au lieu de `type`), des scalaires (`tags`), des objets (`certifications`) et un objet à plat (`address`).

### Registre des objets

Après un run normalisé, interroguez le dictionnaire :

```sql
SELECT object_type, sql_name, json_path, description
FROM landing_order_registry
ORDER BY object_type, json_path;
```

Chaque ligne relie le **nom logique** (chemin du schéma) au **nom SQL réellement créé** — indispensable quand la limite 63 caractères a déclenché la troncature+hash.

## Scénarios

Chaque scénario vit dans `schemas/<nom>/` et contient :

```
schemas/<nom>/
├── schema.json      # JSON Schema polymorphe
├── scenario.json    # métadonnées : table, pk, headers
└── datas/*.json     # documents à insérer
```

Les tests d'intégration **découvrent automatiquement** tous les scénarios et exécutent le pipeline complet (création, insertion, upsert, vues, registre) sur chacun — ajouter un scénario = ajouter un dossier.

## Tests

```sh
make test               # tests unitaires (sans base)
make test-integration   # E2E contre la base (compose doit tourner)
```

- Unitaires : `internal/service/*_test.go` (colonnes, valeurs, modèle, vues, registre, troncature des noms).
- Intégration (`tests/`, build tag `integration`) : data-driven sur `schemas/*/`, invariants génériques (nb de lignes racine = nb de datas, tables enfants = somme des longueurs de tableaux, vues = nb de lignes enfants, registre = tables + vues).

## Make targets

```sh
make db-up              # démarrer PostgreSQL (compose)
make db-down            # arrêter + purger les volumes
make db-logs           # logs PostgreSQL
make test              # tests unitaires
make test-integration  # tests E2E
make run               # CLI, SQL sur stdout
make demo              # mode plat contre la base
make demo-model        # mode normalisé + vues contre la base
make build|vet|fmt
```

## Architecture

```mermaid
flowchart LR
    subgraph CLI["CLI (cmd/main.go)"]
        flags --> plan[Plan du modèle<br/>internal/service]
        plan --> sql[SQL affiché / exécuté]
    end
    subgraph Bento["Pipeline Bento (pkg/processors)"]
        batch[batch de messages] --> proc[schema_to_table_insert / _upsert]
        proc --> lazycreate[création à la volée<br/>tables + vues + registre]
        lazycreate --> tx[transaction batch<br/>INSERT / UPSERT]
    end
    subgraph PG["PostgreSQL"]
        landing[(landing tables)]
        views[(vues dénormalisées)]
        registry[(registre des objets)]
    end
    sql --> landing
    tx --> landing
    views -.-> landing
    registry -.-> landing
```

```
cmd/main.go      CLI (flags, orchestration create/insert/upsert, exécution PostgreSQL)
internal/service
├── schema.go     parsing JSON Schema, types SQL, sanitisation + limite 63 chars
├── columns.go    plan de colonnes (mode plat)
├── values.go     collecte des valeurs payload → littéraux SQL
├── sql.go        hints CREATE / INSERT / UPSERT (mode plat)
├── model.go      plan du modèle normalisé (tables, STI, discriminanteur)
├── model_ddl.go  DDL tables + vues dénormalisées
├── model_dml.go  DML normalisé (+ DELETE deepest-first, navigation JSON)
└── registry.go   table de registre des objets
pkg/processors    processeurs Bento (insert/upsert), doc dans PROCESSORS.md
tests/integration_test.go   tests E2E data-driven
schemas/                    scénarios (order, employee)
```