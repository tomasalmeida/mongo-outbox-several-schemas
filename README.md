# MongoDB → Confluent Cloud Outbox — one topic, many strongly-typed schemas

Two Terraform setups that implement the **transactional outbox pattern** from a
**MongoDB Atlas** source to **Confluent Cloud**, where a **single topic carries
multiple event types** (`typeA`, `typeB`, `typeC`), each serialized as **JSON
Schema (JSON_SR)** against its own strongly-typed schema.

They differ only in **how a record is mapped to a Schema Registry subject** — and
that one choice turns out to make or break the design on a fully-managed connector.

> ## ⚠️ Demo / educational repository — NOT production ready
>
> Explores the pattern; not hardened. Payloads are not validated at the connector,
> secrets/state are handled for local convenience, clusters are single-zone, and
> schemas are examples. Review and harden before real use. **As-is, no warranty.**

## The problem

Your MongoDB outbox collection holds documents shaped like:

```json
{ "schemaName": "typeA", "payload": { "id": "a-1001", "createdAt": "...", "amount": 42.50 } }
{ "schemaName": "typeB", "payload": { "id": "b-2002", "customerId": "cust-9", "status": "OPEN" } }
{ "schemaName": "typeC", "payload": { "id": "c-3003", "eventType": "ORDER_CREATED", ... } }
```

Goals:
- All types land on **one topic**.
- Only the **`payload`** is written to Kafka (the `schemaName` discriminator stays
  in Mongo); the **record key is `payload.id`**.
- Each type is serialized/validated against a **strongly-typed, independently
  versioned JSON Schema**.

The open question is the **subject name strategy** — how each record picks its
Schema Registry subject:

| Strategy | Subject | How types are separated |
|---|---|---|
| `TopicNameStrategy` | `<topic>-value` (one) | one **umbrella** `oneOf` schema referencing the per-type schemas |
| `TopicRecordNameStrategy` | `<topic>-<recordName>` (one per type) | each record's **schema name** (`typeA`…) selects its own subject |

## The two approaches

### 1. [`with-topicnamestrategy/`](with-topicnamestrategy/) — ✅ works (recommended)

One fully-managed connector, default `TopicNameStrategy`, and a single **umbrella
`oneOf`** schema at `<topic>-value` that references `typeA`/`typeB`/`typeC`. A
stock `ExtractField` SMT keeps only the `payload`. No custom code. Consumers
disambiguate types via the `oneOf`.

→ **[Read the guide](with-topicnamestrategy/README.md)**

### 2. [`with-topicrecordstrategy/`](with-topicrecordstrategy/) — ❌ does not work on a fully-managed connector

Aims for **per-type subjects** (`<topic>-typeA/-typeB/-typeC`) from a **single**
connector using `TopicRecordNameStrategy`. That needs the value schema to carry a
per-record **name**, which requires a **custom SMT** (`SetSchemaNameFromField`)
that stamps the schema name from the `schemaName` field.

It fails at serialization:

```
SerializationException: In configuration value.subject.name.strategy =
TopicRecordNameStrategy, the message value must only be a record schema
```

**Why:** on a fully-managed connector the custom SMT runs **out-of-process** (a
Confluent "cloud function"). The SMT *does* set the name correctly, but the
boundary **strips the Connect schema name** before the JSON_SR serializer runs, so
`TopicRecordNameStrategy` has no record name. This is not fixable from the SMT or
any connector/converter config. The folder documents the full investigation.

→ **[Read the guide + conclusion](with-topicrecordstrategy/README.md)**

## Key takeaway

- Want **one fully-managed connector**? Use **`TopicNameStrategy` + umbrella**
  (approach 1).
- Want **per-type subjects via `TopicRecordNameStrategy`**? Either run **one
  fully-managed connector per type** with the stock, in-process
  `SetSchemaMetadata$Value` (static name), **or** run the custom SMT **in-process**
  (self-managed Connect / Confluent Cloud **Custom Connector**) — because the
  managed runtime's out-of-process SMT boundary drops the schema name.

## Prerequisites (both)

- [Terraform](https://developer.hashicorp.com/terraform/downloads) ≥ 1.3
- A Confluent Cloud account + an org-level Cloud API key
  (`confluent api-key create --resource cloud`)
- A MongoDB Atlas cluster (database user, network access for Confluent Cloud, SRV host)

Each folder has its own `terraform.tfvars.example`, usage steps, and details.
