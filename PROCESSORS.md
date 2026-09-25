# Processeurs Bento : `schema_to_table_insert` et `schema_to_table_upsert`

Deux processeurs batch pour [Bento](https://warpstreamlabs.github.io/bento/) (plugins Go intégrés via le module `public/service`) qui stockent les documents JSON entrants dans des landing tables PostgreSQL générées à la volée depuis le JSON Schema annoncé par le message.

Ils réutilisent le moteur du CLI (`internal/normalized.Plan`, primitives dans `internal/schema`) : **mode normalisé uniquement** — tables typées par niveau de tableau + vues dénormalisées `v_<racine>_<chemin>` + registre `<racine>_registry`.

## Enregistrement

Le package `pkg/processors` s'auto-enregistre dès qu'il est importé :

```go
import _ "github.com/laurentpoirierfr/schema-to-table/pkg/processors"
```

Une fois l'import présent, les processeurs `schema_to_table_insert` et `schema_to_table_upsert` sont disponibles dans toute config Bento du même binaire.

## Configuration

| Champ | Défaut | Description |
|-------|--------|-------------|
| `dsn` | *(requis)* | DSN PostgreSQL (`postgres://user:pass@host:5432/db?sslmode=disable`) |
| `schema_url_header` | `schema_url` | nom du **métadata** portant l'URL `http(s)` du JSON Schema (draft 2020-12) |
| `table_name_header` | `table_name` | nom du métadata portant le nom de la table racine |
| `pk` | `id` | colonne(s) de clé de la table racine — cible `ON CONFLICT` de l'upsert |
| `headers` | `{}` | colonnes d'ingestion : `source: TEXT`, `trace_id: TEXT` → deviennent `header_source`, `header_trace_id` |
| `create_model` | `true` | crée tables + vues + registre si la table racine n'existe pas |
| `schema_ttl` | `1h` | durée de vie du cache des schémas par URL ; `0` désactive le cache (fetch à chaque message) |

```yaml
pipeline:
  processors:
    - schema_to_table_insert:
        dsn: postgres://s2t:s2t@localhost:5432/s2t?sslmode=disable
        schema_url_header: schema_url
        table_name_header: table_name
        pk: id
        headers:
          source: TEXT
          trace_id: TEXT
        create_model: true
        schema_ttl: 1h
```

Chaque message doit porter en **métadonnées** les valeurs `schema_url_header` et `table_name_header` (via les labels du input, `set_meta`, ou une mutation préalable) — voir la section dédiée ci-dessous pour `headers`.

### `headers` : métadonnées du message → colonnes typées

Le bloc `headers` déclare, parmi les **métadonnées techniques** du message (les « headers » Bento, posés par l'input, `set_meta`, ou une mutation), celles que l'on veut **persister et typer** en base :

```yaml
headers:            # clé = nom de la métadonnée, valeur = type SQL
  source: TEXT
  trace_id: TEXT
```

1. **À la création du modèle**, chaque clé devient une colonne `header_<clé>` **sur la table racine uniquement**, avec le type SQL annoncé — les tables enfants et les vues dénormalisées ne la reçoivent pas :

   ```sql
   CREATE TABLE "landing_test" (
       "header_source"   TEXT,
       "header_trace_id" TEXT,
       "id"              BIGINT, ...
   )
   ```

2. **À l'écriture de chaque message**, la valeur est lue dans les métadonnées du message (`MetaGet("source")` → `"api"`, …) et insérée dans la colonne correspondante :
   - métadonnée **présente** → sa valeur est stockée ;
   - métadonnée **absente** → la colonne reçoit la chaîne vide `''` (jamais une erreur).

3. **Seules les clés déclarées sont stockées** : `schema_url` et `table_name` sont consommées pour **router** le message (choix du schéma et de la table racine) mais ne deviennent pas des colonnes ; tout autre métadonnée non déclarée est ignorée.

Nuances :

- Les valeurs sont toujours émises comme **littéral de chaîne** (`'api'`, `'t-0001'`) — le type SQL annoncé sert à la **définition de la colonne** et PostgreSQL **caste à l'insertion**. Ex. : `trace_id: UUID` avec `t-0001` fonctionne ; préférez `TEXT` pour des valeurs libres.
- Les types SQL sont ceux du moteur du CLI : `UUID`, `BIGINT`, `NUMERIC`, `BOOLEAN`, `TIMESTAMPTZ`, `TEXT`, …
- Lecture en SQL : `SELECT "header_trace_id" FROM landing_test WHERE id = 1;` (vérifié par le test d'intégration).

## Comportement

- **Transactionnel** : un batch = une transaction, `BEGIN` → création éventuelle du modèle → `INSERT`/`UPSERT` par message → `COMMIT`. La moindre erreur (schéma injoignable, payload invalide, PK absente, échec SQL) → `ROLLBACK` : **aucune écriture partielle**, la ligne fautive est repassée en erreur avec `message.SetError`.
- **Création à la volée** : pour chaque table racine distincte du batch, la présence de la table est vérifiée une fois ; si absente, l'ensemble du modèle est créé — table(s) du schéma, **vues dénormalisées** `v_<racine>_<chemin>` et **registre** `<racine>_registry`. Les erreurs « already exists » (concurrence entre workers) sont tolérées.
- **Upsert** : `ON CONFLICT (<pk>)` sur la table racine + remplacement de la scène complète des enregistrements enfants (les tableaux sans clé naturelle sont remplacés).
- **Types typés** : `uuid` → `UUID`, `integer` → `BIGINT`, `number` → `NUMERIC`, `boolean` → `BOOLEAN`, `date-time` → `TIMESTAMPTZ`, chaînes → `TEXT`, identifiants longs tronqués à 63 caractères (prefixe + hash).
- **Cache du schéma** : le processeur met en cache par URL le JSON Schema téléchargé (timeout HTTP 30 s, taille max 1 Mio, accès sécurisé par mutex). La durée de vie est pilotée par `schema_ttl` : tant qu'une entrée n'a pas expiré, le document est servi depuis le cache ; à expiration (`1h` par défaut) il est re-téléchargé. `schema_ttl: 0` désactive le cache.

## Architecture

```mermaid
flowchart LR
    subgraph Bento
        IN[input batch de messages] --> P[schema_to_table_insert / _upsert]
        P --> OUT[output PostgreSQL]
    end

    P --> M{transaction batch}
    M --> L[loader de schémas<br/>cache HTTP par URL]
    L --> PL[normalized.Plan<br/>internal/normalized]
    M --> DDL{racine existante ?}
    DDL -- non --> D1[DDL tables typées]
    D1 --> D2[Vues dénormalisées v_racine_chemin]
    D2 --> D3[Registre racine_registry]
    DDL -- oui --> DML[DML INSERT / UPSERT par message]
    D3 --> DML
    DML --> C{COMMIT}
    C -- erreur --> RB[ROLLBACK<br/>SetError + batch en échec]

    subgraph PG["PostgreSQL"]
        landing[(landing tables)]
        views[(vues dénormalisées)]
        registry[(registre des objets)]
    end
    OUT --> PG
    DML --> landing
    D2 --> views
    D3 --> registry
```

## Tests

```sh
go test ./pkg/processors                                   # unitaires (fake sink, sans base)
go test -tags integration ./pkg/processors -run TestProcessorsIntegration   # E2E contre compose
```

- **Unitaires** : enregistrement, validation de config, tolérance `already exists`, création/désactivation du modèle, dml insert/upsert, colonnes `header_*`, batchs multi-tables, rollback intégral, schéma trop gros, métadonnées manquantes, erreurs `begin`/`commit`/connectivité.
- **Intégration** : contre la base de `compose.yaml` — création du modèle réel, passages INSERT puis UPSERT, présence des vues et du registre, relecture des valeurs `header_source`/`header_trace_id`, et **rollback intégral** : un batch en échec (double clé primaire) ne laisse ni ligne ni table derrière lui (le DDL du modèle roulback avec la transaction).

(`S2T_DSN` permet de surcharger le DSN par défaut `postgres://s2t:s2t@localhost:5432/s2t?sslmode=disable`.)