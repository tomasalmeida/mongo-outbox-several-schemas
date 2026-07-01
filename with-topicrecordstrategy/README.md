# MongoDB → Confluent Cloud Outbox (TopicRecordNameStrategy + custom SMT)

> ## ⚠️ Demo / educational repository — NOT production ready
>
> This project exists to **explore and demonstrate** an outbox pattern from
> MongoDB Atlas to Confluent Cloud. It is **not** hardened for production use.
> Among other things: payloads are **not validated** at the connector (bad
> events reach the topic), the custom SMT is a minimal example, secrets/state are
> handled for local convenience only, the cluster is a single-zone tier, and
> schemas are **examples**. Review, harden, and test thoroughly before using any
> of this with real data. Provided **as-is, without warranty** of any kind.

Terraform + a custom Single Message Transform (SMT) that put **multiple event
types on one topic**, each serialized against its **own JSON Schema subject**
via **`TopicRecordNameStrategy`** — no umbrella `oneOf` schema.

This is the sibling of [`../with-topicnamestrategy`](../with-topicnamestrategy),
which solves the same problem with a single umbrella schema under
`TopicNameStrategy`. **The only interesting difference is how the subject is
chosen** — see [How this differs from the TopicNameStrategy
variant](#how-this-differs-from-the-topicnamestrategy-variant).

## Contents

- [The core idea](#the-core-idea)
- [Why a custom SMT is required](#why-a-custom-smt-is-required)
- [The custom SMT](#the-custom-smt)
- [Building & deploying the SMT](#building--deploying-the-smt)
- [What gets created](#what-gets-created)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Usage](#usage)
- [Testing end to end](#testing-end-to-end)
- [Consume with the Confluent CLI](#consume-with-the-confluent-cli)
- [What happens to a bad event ⚠️](#what-happens-to-a-bad-event-️)
- [How this differs from the TopicNameStrategy variant](#how-this-differs-from-the-topicnamestrategy-variant)
- [Cleanup](#cleanup)
- [File layout](#file-layout)

## The core idea

Your MongoDB outbox collection holds documents shaped like:

```json
{ "schemaName": "typeA", "payload": { "id": "a-1001", "createdAt": "2026-06-30T10:15:00Z", "amount": 42.50 } }
{ "schemaName": "typeB", "payload": { "id": "b-2002", "customerId": "cust-9", "status": "OPEN" } }
{ "schemaName": "typeC", "payload": { "id": "c-3003", "eventType": "ORDER_CREATED", "occurredAt": "2026-06-30T10:16:30Z", "metadata": { "source": "checkout", "region": "eu-west-1" } } }
```

All three go to **one topic**, but with **`TopicRecordNameStrategy`** the subject
each record serializes against is derived from the **record's schema name**:

```
subject = <topic>-<recordName>
        = outbox-typeA   /   outbox-typeB   /   outbox-typeC
```

So each type is validated against its **own** subject — the `oneOf` umbrella from
the sibling variant is gone. As before, **only the `payload` is written** to
Kafka (the `schemaName` discriminator stays in Mongo), and the **record key is
`payload.id`**.

## Why a custom SMT is required

`TopicRecordNameStrategy` builds the subject from the **name of the value's
schema**. But the fully-managed MongoDB Atlas connector emits a value whose
Connect schema has **no name**, so the strategy fails with *"the message value
must only be a record schema"* / an empty record name. That is exactly the wall
the sibling README hits and works around with an umbrella.

The stock `org.apache.kafka.connect.transforms.SetSchemaMetadata` can set a
schema name — but only a **static** one from config. We need the name to come
from each document's `schemaName` field. Hence a **custom SMT**.

Two subtleties force the design (both are called out in the code):

1. **Naming and payload-promotion must be one SMT.** If you named the whole
   document and *then* used stock `ExtractField` to keep only `payload`, the name
   would be dropped (ExtractField returns the payload's own unnamed schema). You
   also can't extract `payload` *first*, because then `schemaName` is gone. So a
   single SMT reads `schemaName`, promotes `payload`, and names the promoted
   value's schema.
2. **The value must be schema'd.** A schemaless value can't carry a name, so it
   can't drive `TopicRecordNameStrategy` at all — the SMT requires a `Struct`
   (which `output.data.format = JSON_SR` produces) and throws a clear error
   otherwise.

## The custom SMT

[`smt/src/main/java/io/confluent/examples/outbox/transforms/SetSchemaNameFromField.java`](smt/src/main/java/io/confluent/examples/outbox/transforms/SetSchemaNameFromField.java)

Modelled on `SetSchemaMetadata`, but the name is read **per record from a
field**:

| Config | Default | Meaning |
|---|---|---|
| `name.field`  | `schemaName` | The field whose **value** becomes the schema name (the discriminator). |
| `value.field` | `payload`    | Nested field promoted to be the new record value. Empty = rename the whole value in place. |

Effect on a `typeA` document:

```
value in : Struct{ schemaName:"typeA", payload:Struct{ id, createdAt, amount } }   (payload schema unnamed)
value out: Struct{ id, createdAt, amount }                                          (schema name = "typeA")
```

The JSON_SR serializer then maps the Connect schema name → the JSON Schema
`title`, and `TopicRecordNameStrategy` produces subject `<topic>-typeA`. With
`auto.register.schemas=false` + `use.latest.version=true` it fetches the
pre-registered schema at that subject.

> The registered [`schemas/typeA.json`](schemas/typeA.json) etc. keep
> `"title": "typeA"` so the schema name the SMT stamps matches the registered
> subject's schema.

## Building & deploying the SMT

Fully-managed connectors **do** accept custom SMTs, but the plugin must first be
uploaded to the environment as a **Connect artifact**. Until then the connector
config is rejected with:

```
invalid transforms selected: allowed transforms for organization: [ ...built-ins only... ]
```

Uploading the artifact adds your custom class to that allowed list. There is **no
Terraform resource** for the upload, so it is a CLI step
([Confluent docs: custom SMT quick start](https://docs.confluent.io/cloud/current/connectors/configure-custom-single-message-transforms/quick-start-custom-smt.html)).

Two things about that artifact trip up the connector:

- **It must be `READY`.** Right after upload it is `PROCESSING` while Confluent
  scans the JAR; the custom class is only added to the connector's allowed
  transforms once it flips to `READY`. Creating the connector while it is still
  `PROCESSING` produces the same `invalid transforms selected` 400.
- **The connector binds it by class name *and* artifact ID.** The control plane
  detects custom-SMT usage from `custom.smt.artifact.id` and uses it to provision
  the runtime path — the class name alone is not enough. This Terraform sets both
  (the ID comes from `var.smt_artifact_id`, which you set after uploading):

  ```hcl
  "transforms.NamePayload.type"                   = "io.confluent.examples.outbox.transforms.SetSchemaNameFromField"
  "transforms.NamePayload.custom.smt.artifact.id" = var.smt_artifact_id   # e.g. "cca-xxxxxx"
  ```

**The jar must be an uber jar bundling `com.google.re2j` (RE2/J).** The
custom-SMT sandbox validates the schema name this SMT sets using
`com.google.re2j.Pattern` but does **not** put re2j on the function classpath —
so a bare jar passes validation and then dies at provisioning with
`NoClassDefFoundError: com/google/re2j/Pattern` (empty tasks, generic "contact
Support"). The [`smt/pom.xml`](smt/pom.xml) bundles re2j via the shade plugin;
`mvn package` produces the fat jar. (SMTs that never touch schema names don't hit
this — which is why the reference `InsertUuid` example gets away with a bare jar.)

The build → upload → wait-for-`READY` → set-`smt_artifact_id` commands are all in
[Usage](#usage).

> Prefer no custom code? Run the connector **self-managed** / as a Confluent
> Cloud **custom connector** with the JAR on the plugin path (wiring differs from
> these fully-managed props), or use one fully-managed connector per type with
> the stock `SetSchemaMetadata$Value` (static name).

## What gets created

| Resource | Count | Notes |
|---|---|---|
| Environment | 1 | Stream Governance (Schema Registry) enabled |
| Kafka cluster | 1 | Standard, AWS `eu-west-1` (configurable) |
| Kafka topic | 1 | Named after the source collection |
| Service accounts | 2 | `app-manager` (Terraform), `connector` (runs the connector) |
| API keys | 2 | Kafka + Schema Registry, owned by `app-manager` |
| JSON schemas | 3 | One subject **per type**: `<topic>-typeA/-typeB/-typeC` (no umbrella) |
| MongoDB Atlas source connector | 1 | Single connector, using the custom SMT |
| Custom SMT | 1 | Built with Maven, uploaded via `confluent connect artifact create` — **not** managed by Terraform |

## Architecture

```
                MongoDB Atlas collection (var.mongodb_collection)
                { schemaName, payload }
                              │  change stream: inserts only
                              ▼
            ┌────────────────────────────────────────────────────────┐
            │ single MongoDbAtlasSource connector                    │
            │  NamePayload (CUSTOM SMT)                               │
            │     read schemaName -> set value schema name           │
            │     promote payload -> value = payload                 │
            │  CopyIdToKey / ExtractKeyId -> key = payload.id         │
            │  RouteToOutbox              -> topic = <collection>     │
            │  output.data.format = JSON_SR                          │
            │  value subject strategy = TopicRecordNameStrategy      │
            │  auto.register=false, use.latest.version=true          │
            └────────────────────────────────────────────────────────┘
                              ▼
                  topic: <collection>
                              │  subject per record: <collection>-<schemaName>
                              ▼
       Schema Registry (no umbrella):
         <collection>-typeA   = typeA payload schema
         <collection>-typeB   = typeB payload schema
         <collection>-typeC   = typeC payload schema
```

## Prerequisites

- [Terraform](https://developer.hashicorp.com/terraform/downloads) ≥ 1.3
- **Java 8+ and Maven** (to build the custom SMT)
- A Confluent Cloud account and an **org-level Cloud API key**
  (`confluent api-key create --resource cloud`)
- The **Confluent CLI**, logged in (`confluent login`) — used to upload the
  custom SMT as a Connect artifact
- A MongoDB Atlas cluster with a read-capable database user, network access for
  Confluent Cloud, and the SRV host (e.g. `cluster0.abcde.mongodb.net`)

## Usage

The SMT artifact must be uploaded and `READY` **before** the connector is
created, and the upload needs the environment ID that Terraform creates — so
apply in two phases, with the artifact build/upload in between:

```bash
cp terraform.tfvars.example terraform.tfvars
# edit terraform.tfvars: Cloud API key/secret + MongoDB host/user/password
#                        + mongodb_database / mongodb_collection

terraform init
confluent login

# --- Phase 1: create the environment (gives us an ID to upload the artifact into) ---
terraform apply -target=confluent_environment.this
ENV="$(terraform output -raw environment_id)"

# --- Build + upload the custom SMT as a Connect artifact (--cloud must match the cluster) ---
(cd smt && mvn -q clean package)          # -> smt/target/outbox-smt-1.0.0.jar
confluent connect artifact create outbox-smt \
  --artifact-file smt/target/outbox-smt-1.0.0.jar \
  --cloud aws \
  --environment "$ENV" \
  --description "SetSchemaNameFromField SMT (outbox record-name-strategy demo)"

# list artifacts (each re-upload adds a new one, so match by name below):
confluent connect artifact list --cloud aws --environment "$ENV"

# --- Wait until the outbox-smt artifact resolves and is READY (starts PROCESSING; connector 400s until then) ---
# Filter by name so extra artifacts from earlier uploads don't confuse the lookup;
# if several share the name, take the newest.
until
  ART=$(confluent connect artifact list --cloud aws --environment "$ENV" -o json \
    | jq -r '[.[] | select(.name=="outbox-smt")] | last | .id // empty')
  [ -n "$ART" ] && confluent connect artifact describe "$ART" --cloud aws --environment "$ENV" -o json \
    | jq -e '.status == "READY"' >/dev/null
do
  echo "SMT not READY yet (ART='$ART'); waiting..."; sleep 10
done
echo "SMT $ART is READY."

# --- Bind the artifact ID (via TF_VAR_), then create everything else incl. the connector ---
export TF_VAR_smt_artifact_id="$ART"
terraform apply
```

> Already ran a full `terraform apply` and only the connector failed with
> `invalid transforms selected`? Everything else is created — just do the
> build/upload, wait for `READY`, `export TF_VAR_smt_artifact_id=<id>`, and
> re-run `terraform apply`.

Useful outputs:

```bash
terraform output outbox_topic          # the topic name (= collection)
terraform output type_schema_subjects  # ["<topic>-typeA","<topic>-typeB","<topic>-typeC"]
terraform output connector_id
```

## Testing end to end

1. Insert documents via the **Atlas Data Explorer** (same documents as the
   sibling variant — the `{ schemaName, payload }` shape above).
2. Console → **Topics → `<collection>` → Messages**: confirm the three payloads
   arrive, each keyed by its `id`.
3. **Schema Registry**: confirm the **per-type** subjects `<collection>-typeA`,
   `-typeB`, `-typeC` exist — and that there is **no** `<collection>-value`
   umbrella.

## Consume with the Confluent CLI

Records are JSON_SR, so deserialize with `--value-format jsonschema`. The
deserializer reads the schema **ID** from each record's wire bytes and fetches
that schema — so this works identically regardless of subject strategy:

```bash
confluent login
confluent environment use  "$(terraform output -raw environment_id)"
confluent kafka cluster use "$(terraform output -raw kafka_cluster_id)"

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

Unlike the umbrella variant, here **each record carries a different schema ID**
(one per type), so a consumer knows the type directly from the schema rather than
by matching a `oneOf` branch.

## What happens to a bad event ⚠️

Same caveat as the sibling variant: the fully-managed MongoDB **source**
connector does **not** enforce `value.converter.json.fail.invalid.schema=true`.
With `use.latest.version=true` it stamps the (per-type) schema ID **without
validating** the data, so a malformed `payload` still reaches the topic tagged
with a valid schema ID. Source connectors have **no DLQ**.

The reliable fix is to **gate at the source** with a MongoDB `$jsonSchema`
collection validator so Mongo refuses the bad insert — see the sibling README's
["How to actually reject bad events"](../with-topicnamestrategy/README.md#how-to-actually-reject-bad-events).
Here the SMT adds one extra failure mode: a document with a `schemaName` that has
no matching registered subject will fail serialization and be dropped (logged) —
so the source validator should also constrain `schemaName` to the known types.

## How this differs from the TopicNameStrategy variant

| | [`../with-topicnamestrategy`](../with-topicnamestrategy) | this variant |
|---|---|---|
| Subject strategy | `TopicNameStrategy` | `TopicRecordNameStrategy` |
| Subjects | one: `<topic>-value` | one per type: `<topic>-typeA/-typeB/-typeC` |
| Umbrella `oneOf` schema | ✅ required | ❌ none |
| Schema per record | same umbrella ID for all | distinct ID per type |
| Custom SMT | not needed | **required** (to name the value schema) |
| Deploy complexity | `terraform apply` only | build + upload SMT, then `terraform apply` |
| Type disambiguation (consumer) | by payload shape (`oneOf` + `additionalProperties:false`) | by schema ID directly |

Pick `TopicNameStrategy` for the simplest deploy; pick `TopicRecordNameStrategy`
when you want each type to be a first-class, independently-versioned subject and
consumers to know the type from the schema ID rather than by matching a branch.

## Cleanup

```bash
terraform destroy
# then remove the uploaded custom transform from Confluent Cloud (manual)
```

## File layout

| File | Purpose |
|---|---|
| [versions.tf](versions.tf) | Terraform & provider version constraints |
| [providers.tf](providers.tf) | Confluent provider auth |
| [variables.tf](variables.tf) | Input variables |
| [locals.tf](locals.tf) | `topic_name` derived from the collection |
| [main.tf](main.tf) | Environment, cluster, Schema Registry, topic |
| [service-accounts.tf](service-accounts.tf) | Service accounts, role bindings, API keys |
| [schemas.tf](schemas.tf) | Registers the 3 per-type subjects (`<topic>-typeX`) |
| [connector.tf](connector.tf) | The single MongoDB Atlas source connector + custom SMT wiring |
| [outputs.tf](outputs.tf) | Outputs (IDs, endpoints, subjects, credentials) |
| [schemas/](schemas/) | `typeA/B/C.json` payload schemas (no umbrella) |
| [smt/](smt/) | The custom `SetSchemaNameFromField` SMT (Maven project) |
| [terraform.tfvars.example](terraform.tfvars.example) | Template for your variables |
