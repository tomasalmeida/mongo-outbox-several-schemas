package io.confluent.examples.outbox.transforms;

import io.confluent.connect.json.JsonSchemaData;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.serializers.subject.TopicRecordNameStrategy;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.Test;

/**
 * Reproduces the exact serialization path the connector uses:
 *   Connect Schema  --JsonSchemaData.fromConnectSchema-->  JsonSchema  --TopicRecordNameStrategy-->  subject
 *
 * Goal: determine whether a *named* Connect Struct becomes a JSON Schema whose name/title
 * TopicRecordNameStrategy accepts — i.e. whether the "must only be a record schema" error is a
 * JsonSchemaData/converter behavior (fixable) or purely a runtime/marshaling issue (not fixable
 * in the SMT). Prints observations; asserts nothing so we always see the output.
 */
public class SchemaNamePathTest {

    private static void probe(String label, Schema connectSchema) {
        System.out.println("\n================ " + label + " ================");
        System.out.println("connectSchema.name() = " + connectSchema.name());

        final JsonSchemaData data = new JsonSchemaData();
        final JsonSchema jsonSchema = data.fromConnectSchema(connectSchema);

        System.out.println("jsonSchema.name()    = " + jsonSchema.name());
        System.out.println("canonical            = " + jsonSchema.canonicalString());

        try {
            final String subject = new TopicRecordNameStrategy()
                .subjectName("tomas.outbox", false, jsonSchema);
            System.out.println("subject              = " + subject + "   <-- OK");
        } catch (Exception e) {
            System.out.println("subjectName THREW    = " + e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    @Test
    public void namedStructPath() {
        // 1) A plainly-named struct (what our SMT is supposed to produce).
        probe("named struct 'typeA'",
            SchemaBuilder.struct().name("typeA")
                .field("id", Schema.STRING_SCHEMA)
                .field("amount", Schema.FLOAT64_SCHEMA)
                .build());

        // 2) A fully-qualified name, in case RecordNameStrategy wants a namespaced record name.
        probe("named struct 'io.confluent.examples.outbox.typeA'",
            SchemaBuilder.struct().name("io.confluent.examples.outbox.typeA")
                .field("id", Schema.STRING_SCHEMA)
                .field("amount", Schema.FLOAT64_SCHEMA)
                .build());

        // 3) An UNNAMED struct — the failure baseline.
        probe("unnamed struct",
            SchemaBuilder.struct()
                .field("id", Schema.STRING_SCHEMA)
                .field("amount", Schema.FLOAT64_SCHEMA)
                .build());
    }
}
