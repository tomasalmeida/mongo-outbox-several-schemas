# MongoDB → Confluent Cloud Outbox (JSON Schema references)

> ## ⚠️ Demo / educational repository — NOT production ready
>
> This project exists to **explore and demonstrate** an outbox pattern from
> MongoDB Atlas to Confluent Cloud. It is **not** hardened for production use.
> Among other things: payloads are **not validated** at the connector (bad
> events reach the topic), secrets/state are handled for local convenience only,
> the cluster is a single-zone Basic tier, schemas are **examples**, and error
> handling drops rejected records. Review, harden, and test thoroughly before
> using any of this with real data or workloads. Provided **as-is, without
> warranty** of any kind.

Terraform that provisions a complete Confluent Cloud setup to implement the
**transactional outbox pattern** from a **MongoDB Atlas** source. A single topic
carries **multiple event types** (`typeA`, `typeB`, `typeC`), serialized as JSON
Schema against a single **umbrella schema** that references the three reusable
per-type schemas.

> ⚠️ Note: the fully-managed source connector does **not** validate payloads
> against the schema, so malformed events still reach the topic. To reject them,
> validate at the Mongo side — see
> [What happens to a bad event](#what-happens-to-a-bad-event-️).

## Contents

- [What gets created](#what-gets-created)
- [The core idea](#the-core-idea)
  - [The three event types](#the-three-event-types)
  - [Why one connector + an umbrella schema (and not `TopicRecordNameStrategy`)](#why-one-connector--an-umbrella-schema-and-not-topicrecordnamestrategy)
  - [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Usage](#usage)
- [Testing end to end](#testing-end-to-end)
- [Consume with the Confluent CLI](#consume-with-the-confluent-cli)
  - [Seeing the schema ID](#seeing-the-schema-id)
- [What happens to a bad event ⚠️](#what-happens-to-a-bad-event-️)
  - [How to actually reject bad events](#how-to-actually-reject-bad-events)
- [Customizing the schemas](#customizing-the-schemas)
- [Cleanup](#cleanup)
- [Notes, assumptions & gotchas](#notes-assumptions--gotchas)
- [File layout](#file-layout)

## What gets created

| Resource | Count | Notes |
|---|---|---|
| Environment | 1 | Stream Governance (Schema Registry) enabled |
| Kafka cluster | 1 | Basic, AWS `eu-west-1` (configurable) |
| Kafka topic | 1 | Named after the source collection (`local.topic_name = var.mongodb_collection`) |
| Service accounts | 2 | `app-manager` (Terraform), `connector` (runs the connector) |
| API keys | 2 | Kafka + Schema Registry, owned by `app-manager` |
| JSON schemas | 4 | 3 type subjects (`typeA`/`typeB`/`typeC`) + 1 umbrella (`<topic>-value`) |
| MongoDB Atlas source connector | 1 | Single connector for all types |

## The core idea

Your MongoDB outbox collection holds documents shaped like:

```json
{ "schemaName": "typeA", "payload": { ... } }
{ "schemaName": "typeB", "payload": { ... } }
{ "schemaName": "typeC", "payload": { ... } }
```

We want all of these on **one topic**, serialized against **strongly-typed,
reusable schemas**. We do that with **JSON Schema references**: each type is
registered as its own subject, and an **umbrella** schema (`oneOf` of `$ref`s)
is what the connector serializes against.

Two important properties:

- **Only the `payload` object is serialized to Kafka.** `schemaName` stays in
  Mongo and is dropped by an `ExtractField` SMT — it is *not* part of any
  Confluent schema.
- **The record key is `payload.id`** (set via `ValueToKey` + `ExtractField$Key`).

### The three event types

For each type: the **Mongo document** (what you insert) vs the **Kafka value**
(what lands on the topic). The per-type schemas live in
[`schemas/`](schemas/); the umbrella is [`schemas/outboxEvent.json`](schemas/outboxEvent.json).

#### `typeA`

Mongo document:

```json
{ "schemaName": "typeA", "payload": { "id": "a-1001", "createdAt": "2026-06-30T10:15:00Z", "amount": 42.50 } }
```

Kafka value (payload only, key = `a-1001`):

```json
{ "id": "a-1001", "createdAt": "2026-06-30T10:15:00Z", "amount": 42.50 }
```

#### `typeB`

Mongo document:

```json
{ "schemaName": "typeB", "payload": { "id": "b-2002", "customerId": "cust-9", "status": "OPEN" } }
```

Kafka value (payload only, key = `b-2002`):

```json
{ "id": "b-2002", "customerId": "cust-9", "status": "OPEN" }
```

#### `typeC`

Mongo document:

```json
{ "schemaName": "typeC", "payload": { "id": "c-3003", "eventType": "ORDER_CREATED", "occurredAt": "2026-06-30T10:16:30Z", "metadata": { "source": "checkout", "region": "eu-west-1" } } }
```

Kafka value (payload only, key = `c-3003`):

```json
{ "id": "c-3003", "eventType": "ORDER_CREATED", "occurredAt": "2026-06-30T10:16:30Z", "metadata": { "source": "checkout", "region": "eu-west-1" } }
```

### Why one connector + an umbrella schema (and not `TopicRecordNameStrategy`)

The first instinct is `TopicRecordNameStrategy` (subject = `<topic>-<recordName>`,
many types on one topic). It does **not** work here: that strategy reads the
record's *schema name*, but the MongoDB connector emits a schemaless value, so
after `ExtractField` there is no named record schema — the serializer throws
*"the message value must only be a record schema"*.

Instead we use the default **`TopicNameStrategy`** (subject = `<topic>-value`)
and register one **umbrella** schema there:

```jsonc
// schemas/outboxEvent.json  -> subject "<topic>-value"
{
  "oneOf": [
    { "$ref": "typeA" },
    { "$ref": "typeB" },
    { "$ref": "typeC" }
  ]
}
```

The `$ref` names are resolved to the registered `typeA`/`typeB`/`typeC` subjects
via `schema_reference` blocks in [`schemas.tf`](schemas.tf). Every record on the
topic is tagged with the umbrella's schema ID; a **consumer** uses the `oneOf`
to deserialize each `payload` as whichever branch it matches — so a single
connector + single subject carries all three reusable, independently-versioned
types.

> **`additionalProperties: false` is load-bearing — for consumers.** Since
> `schemaName` is not in the Kafka value, the `oneOf` is disambiguated purely by
> payload shape: each type schema sets `additionalProperties: false` plus
> distinctive `required` fields so exactly one branch matches on the read side.
> Note the **producer/connector does not enforce this** (see
> [What happens to a bad event](#what-happens-to-a-bad-event-️)) — it matters
> when consumers validate, and if you later add a Mongo-side validator.

### Architecture

```
                MongoDB Atlas collection (var.mongodb_collection)
                { schemaName, payload }
                              │  change stream: inserts only
                              ▼
            ┌────────────────────────────────────────────────┐
            │ single MongoDbAtlasSource connector            │
            │  ExtractPayload   -> value = payload           │
            │  CopyIdToKey      -> key   = payload.id        │
            │  ExtractKeyId     -> flatten key               │
            │  RouteToOutbox    -> topic = <collection>      │
            │  output.data.format = JSON_SR                  │
            │  value subject strategy = TopicNameStrategy    │
            │  auto.register=false, use.latest.version=true  │
            │  json.fail.invalid.schema=true (NOT enforced)  │
            │  errors.tolerance=all                          │
            └────────────────────────────────────────────────┘
                              ▼
                  topic: <collection>   (e.g. "outbox2")
                              │  subject: <collection>-value
                              ▼
       Schema Registry:
         <collection>-value   = umbrella  oneOf( $ref typeA/B/C )
         typeA, typeB, typeC  = referenced payload schemas
```

## Prerequisites

- [Terraform](https://developer.hashicorp.com/terraform/downloads) ≥ 1.3
- A Confluent Cloud account and an **org-level Cloud API key**
  (`confluent api-key create --resource cloud`)
- A MongoDB Atlas cluster with:
  - a database user (read access to the outbox DB),
  - **network access** allowing Confluent Cloud — easiest is to allowlist
    `0.0.0.0/0` for a quick test, or use Atlas Private Link / specific egress
    IPs for production,
  - the SRV host (e.g. `cluster0.abcde.mongodb.net`).

## Usage

```bash
cp terraform.tfvars.example terraform.tfvars
# edit terraform.tfvars: Cloud API key/secret + MongoDB host/user/password
#                        + mongodb_database / mongodb_collection

terraform init
terraform plan
terraform apply
```

Useful outputs:

```bash
terraform output outbox_topic              # the topic name (= collection)
terraform output type_schema_subjects      # ["typeA","typeB","typeC"]
terraform output umbrella_schema_subject   # "<collection>-value"
terraform output connector_id
terraform output -raw app_manager_kafka_api_key
terraform output -raw app_manager_kafka_api_secret
```

## Testing end to end

The topic and database/collection names come from your variables. With the
example values (`mongodb_database = "tomas"`, `mongodb_collection = "outbox2"`)
the topic is `outbox2` and the subject is `outbox2-value`.

1. Insert documents via the **Atlas Data Explorer**: open your cluster →
   **Browse Collections** → your database → your collection → **INSERT DOCUMENT**
   → switch to the `{}` (JSON) view → paste a document (Atlas auto-generates
   `_id`; just paste the fields below) → **Insert**.

   `typeA`:

   ```json
   { "schemaName": "typeA", "payload": { "id": "a-1001", "createdAt": "2026-06-30T10:15:00Z", "amount": 42.50 } }
   ```

   `typeB`:

   ```json
   { "schemaName": "typeB", "payload": { "id": "b-2002", "customerId": "cust-9", "status": "OPEN" } }
   ```

   `typeC`:

   ```json
   { "schemaName": "typeC", "payload": { "id": "c-3003", "eventType": "ORDER_CREATED", "occurredAt": "2026-06-30T10:16:30Z", "metadata": { "source": "checkout", "region": "eu-west-1" } } }
   ```

2. Console → your cluster → **Topics → `<collection>` → Messages** (Jump to
   offset 0): confirm the three payloads arrive, each keyed by its `id`.
3. **Schema Registry**: confirm `typeA`/`typeB`/`typeC` and `<collection>-value`
   subjects exist and that the umbrella references the three.

> `startup.mode` defaults to `copy_existing`, so documents already in the
> collection are snapshotted on first run. Set `connector_startup_mode = "latest"`
> to only capture new inserts.

## Consume with the Confluent CLI

Because records are serialized as **JSON_SR**, tell the CLI to deserialize with
`--value-format jsonschema`: it fetches the umbrella schema (`<topic>-value`)
from Schema Registry, resolves the `oneOf` `$ref`s, and validates each `payload`
against whichever `typeA`/`typeB`/`typeC` branch it matches — a plain
`confluent kafka topic consume` (string) would just print the raw bytes and skip
the schema entirely.

First point the CLI at the environment and cluster this Terraform created:

```bash
confluent login

confluent environment use  "$(terraform output -raw environment_id)"
confluent kafka cluster use "$(terraform output -raw kafka_cluster_id)"
```

Then consume the topic, deserializing values against the schema. The Kafka
credentials authenticate to the cluster; the Schema Registry credentials let the
CLI pull the umbrella schema to decode the JSON_SR payloads:

```bash
confluent kafka topic consume "$(terraform output -raw outbox_topic)" \
  --from-beginning \
  --print-key \
  --value-format jsonschema \
  --api-key    "$(terraform output -raw app_manager_kafka_api_key)" \
  --api-secret "$(terraform output -raw app_manager_kafka_api_secret)" \
  --schema-registry-endpoint   "$(terraform output -raw schema_registry_rest_endpoint)" \
  --schema-registry-api-key    "$(terraform output -raw app_manager_sr_api_key)" \
  --schema-registry-api-secret "$(terraform output -raw app_manager_sr_api_secret)"
```

Add `--print-offset` and `--timestamp` if you also want the partition/offset and
message time printed alongside each record.

With the sample inserts from above you should see each `payload` decoded and
keyed by its `id` (`a-1001`, `b-2002`, `c-3003`):

```text
"a-1001"	{"id":"a-1001","createdAt":"2026-06-30T10:15:00Z","amount":42.5}
"b-2002"	{"id":"b-2002","customerId":"cust-9","status":"OPEN"}
"c-3003"	{"id":"c-3003","eventType":"ORDER_CREATED","occurredAt":"2026-06-30T10:16:30Z","metadata":{"source":"checkout","region":"eu-west-1"}}
```

### Seeing the schema ID

`confluent kafka topic consume` has **no** flag to print the per-record schema
ID (its print options are only `--print-key`, `--print-offset`, `--full-header`,
and `--timestamp`). It doesn't need one here: because the connector runs with
`use.latest.version=true`, **every** record on the topic is stamped with the
*same* umbrella schema ID (`<topic>-value`), so the "schema ID" is a single
constant you look up once from Schema Registry rather than something that varies
per message:

```bash
confluent schema-registry schema describe \
  --subject "$(terraform output -raw umbrella_schema_subject)" \
  --version latest \
  --schema-registry-endpoint   "$(terraform output -raw schema_registry_rest_endpoint)" \
  --schema-registry-api-key    "$(terraform output -raw app_manager_sr_api_key)" \
  --schema-registry-api-secret "$(terraform output -raw app_manager_sr_api_secret)"
```

The printed schema ID is the one embedded in every record's JSON_SR wire bytes
(magic byte `0x00` + 4-byte big-endian ID). If you must confirm it straight off
the wire, consume once with `--value-format string`: the leading bytes are that
same ID before the JSON begins.

> The umbrella `oneOf` is disambiguated purely by payload shape (see
> [`additionalProperties: false` is load-bearing](#why-one-connector--an-umbrella-schema-and-not-topicrecordnamestrategy)),
> since `schemaName` is not part of the Kafka value. A **bad event** (see below)
> is still tagged with the umbrella schema ID, so the CLI will happily print it —
> deserialization against the ID succeeds even though the data matches no branch.

## What happens to a bad event ⚠️

A "bad" event is a `payload` that matches none of the `oneOf` branches (extra
fields, missing required fields, etc.).

**Important: a bad event is NOT rejected at the connector, and WILL land on the
topic.** The connector config sets `value.converter.json.fail.invalid.schema =
true`, but the fully-managed MongoDB Atlas **source** connector does **not**
enforce it: with `use.latest.version = true` the serializer stamps each record
with the umbrella schema ID **without validating** that the data actually
conforms. So a malformed payload is written to the topic anyway, tagged with a
valid schema ID — which is worse than a plain bad message, because consumers
trust the ID.

What this means:

| | Result |
|---|---|
| Bad record on the topic | ⚠️ **Yes** — it gets through |
| Connector/task | ✅ Stays Running |
| DLQ | ❌ None — DLQ is sink-only in Confluent Cloud; source connectors have no DLQ |

There is **no reliable way to reject a non-conforming payload at this source
connector.** (Broker-side Schema Validation doesn't help either — it only checks
that the record references a *registered* schema ID, not that the data matches
the schema.)

### How to actually reject bad events

Gate at the **source**, with a MongoDB `$jsonSchema` collection validator so
Mongo refuses the bad insert before it ever reaches the connector:

```js
db.runCommand({
  collMod: "outbox2",                 // your collection
  validator: { $jsonSchema: {
    bsonType: "object",
    required: ["schemaName", "payload"],
    properties: {
      schemaName: { enum: ["typeA", "typeB", "typeC"] },
      payload:    { bsonType: "object", required: ["id"] }
    }
  }},
  validationLevel: "strict",
  validationAction: "error"
})
```

Mirror your per-type payload rules here (keyed off `schemaName`) to match the
Confluent schemas. Alternatively, stage raw events to one topic and run a
**sink** connector downstream (sinks *do* have a DLQ) to validate and quarantine.

## Customizing the schemas

The files in [`schemas/`](schemas/) are **examples** — replace the payload
properties with your real fields and re-run `terraform apply`. Keep
`additionalProperties: false` so the umbrella `oneOf` stays unambiguous.

**Adding a 4th type:**
1. Add it to `outbox_types` in [variables.tf](variables.tf).
2. Add `schemas/typeD.json` (with `additionalProperties: false`).
3. Add `{ "$ref": "typeD" }` to the `oneOf` in [schemas/outboxEvent.json](schemas/outboxEvent.json).

Terraform registers the new type subject and wires the reference automatically
(the `schema_reference` block iterates over `confluent_schema.type`).

## Cleanup

```bash
terraform destroy
```

## Notes, assumptions & gotchas

- **Topic name = collection name.** `local.topic_name = var.mongodb_collection`
  keeps the topic, the `RouteToOutbox` target, the SR subject, and the topic ACL
  in sync. `RouteToOutbox` rewrites the connector's namespace-based topic
  (`<db>.<collection>`) to the clean collection name.
- **No payload validation at the connector**: `value.converter.json.fail.invalid.schema`
  is set but the fully-managed source connector does **not** enforce it, so
  non-conforming payloads still reach the topic (see
  [What happens to a bad event](#what-happens-to-a-bad-event-️)). Reject at the
  Mongo side with a `$jsonSchema` validator.
- **Schema compatibility**: changing a type schema in a breaking way can be
  rejected by the subject's compatibility (default `BACKWARD`). For dev, delete
  the subject in the Console or set its compatibility to `NONE`, then re-apply.
- **Self-managed differs**: these are **fully-managed** Confluent Cloud property
  names. A self-managed Connect cluster uses
  `com.mongodb.kafka.connect.MongoSourceConnector` and different converter wiring.
- **Single zone / Basic cluster**: cheapest for a demo. For production switch
  `cluster_availability` to `MULTI_ZONE` and consider Standard/Dedicated.
- **Secrets**: `terraform.tfvars`, state, and `*.auto.tfvars` are gitignored.
  State contains secrets — use an encrypted remote backend beyond local testing.
- **Ordering**: because `auto.register.schemas = false`, the schemas must exist
  before the connector serializes — `depends_on` in [connector.tf](connector.tf)
  enforces it.

## File layout

| File | Purpose |
|---|---|
| [versions.tf](versions.tf) | Terraform & provider version constraints |
| [providers.tf](providers.tf) | Confluent provider auth |
| [variables.tf](variables.tf) | Input variables |
| [locals.tf](locals.tf) | `topic_name` derived from the collection |
| [main.tf](main.tf) | Environment, cluster, Schema Registry, topic |
| [service-accounts.tf](service-accounts.tf) | Service accounts, role bindings, API keys |
| [schemas.tf](schemas.tf) | Registers the 3 type schemas + the umbrella (with references) |
| [connector.tf](connector.tf) | The single MongoDB Atlas source connector |
| [outputs.tf](outputs.tf) | Outputs (IDs, endpoints, subjects, credentials) |
| [schemas/](schemas/) | `typeA/B/C.json` payload schemas + `outboxEvent.json` umbrella |
| [terraform.tfvars.example](terraform.tfvars.example) | Template for your variables |
