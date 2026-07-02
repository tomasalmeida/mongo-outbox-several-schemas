package io.confluent.examples.outbox.transforms;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Custom SMT for the outbox pattern, built for {@code TopicRecordNameStrategy}.
 *
 * <p>It sets the <em>value</em> schema's name dynamically, per record, from a field (the outbox
 * {@code schemaName} discriminator) — unlike the stock {@code SetSchemaMetadata}, whose
 * {@code schema.name} is static. {@code TopicRecordNameStrategy} derives the subject from the
 * value schema's name ({@code <topic>-<recordName>}); the MongoDB source emits a value with no
 * schema name, so stamping it (e.g. {@code "typeA"}) targets subject {@code <topic>-typeA}.
 *
 * <p>It also promotes a nested field — {@code payload} — to become the new record value and names
 * that value's schema. Set {@code value.field} to empty to name the whole value in place.
 *
 * <p><b>Handles both shapes the connector may emit:</b>
 * <ul>
 *   <li><b>Schemaless {@code Map}</b> (what the fully-managed MongoDB Atlas source actually
 *       produces — it reads BSON into a {@code Map}; JSON_SR is applied later at the converter):
 *       a named {@code Struct} schema is <em>inferred</em> from the payload map so the value can
 *       carry a name.</li>
 *   <li><b>Schema'd {@code Struct}</b>: the promoted field's schema is copied with the new name.</li>
 * </ul>
 *
 * <p>Config:
 * <ul>
 *   <li>{@code name.field}  (default {@code schemaName}) — field whose value becomes the schema name.</li>
 *   <li>{@code value.field} (default {@code payload})    — nested field promoted to the new value; empty = in place.</li>
 * </ul>
 */
public class SetSchemaNameFromField<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final Logger log = LoggerFactory.getLogger(SetSchemaNameFromField.class);

    public static final String OVERVIEW_DOC =
        "Set the value schema's name from a record field (the outbox 'schemaName' discriminator), "
      + "promoting a nested 'payload' field to be the new value. Works on schemaless (Map) values "
      + "from the MongoDB source by inferring a named Struct, so a single topic can drive "
      + "TopicRecordNameStrategy (subject = <topic>-<schemaName>).";

    public static final String NAME_FIELD_CONFIG  = "name.field";
    public static final String VALUE_FIELD_CONFIG = "value.field";

    private static final String NAME_FIELD_DEFAULT  = "schemaName";
    private static final String VALUE_FIELD_DEFAULT = "payload";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
        .define(NAME_FIELD_CONFIG, ConfigDef.Type.STRING, NAME_FIELD_DEFAULT,
            ConfigDef.Importance.HIGH,
            "Field whose VALUE becomes the schema name (the discriminator).")
        .define(VALUE_FIELD_CONFIG, ConfigDef.Type.STRING, VALUE_FIELD_DEFAULT,
            ConfigDef.Importance.HIGH,
            "Nested field promoted to be the new record value. Leave empty to name the whole "
          + "value in place.");

    private String nameField;
    private String valueField;

    @Override
    public void configure(Map<String, ?> props) {
        final Object nf = props.get(NAME_FIELD_CONFIG);
        final Object vf = props.get(VALUE_FIELD_CONFIG);
        nameField  = nf == null ? NAME_FIELD_DEFAULT : nf.toString();
        valueField = vf == null ? VALUE_FIELD_DEFAULT : vf.toString();
    }

    @Override
    public R apply(R record) {
        log.info(
            "INPUT valueSchema type={}, name={}, valueClass={}",
            record.valueSchema() != null ? record.valueSchema().type() : null,
            record.valueSchema() != null ? record.valueSchema().name() : null,
            record.value() != null ? record.value().getClass() : null);

        final Object value = record.value();
        if (value == null) {
            return record;
        }

        final Schema namedSchema;
        final Struct out;

        if (value instanceof Struct) {
            // --- schema'd path: copy the promoted field's schema with the new name ---
            final Struct root = (Struct) value;
            final Schema rootSchema = record.valueSchema();
            final String schemaName = discriminator(root.get(nameField));

            final Struct sourceStruct;
            final Schema sourceSchema;
            if (valueField.isEmpty()) {
                sourceStruct = root;
                sourceSchema = rootSchema;
            } else {
                final Field f = rootSchema.field(valueField);
                if (f == null) {
                    throw new DataException("Value field '" + valueField + "' not found in the record schema.");
                }
                if (f.schema().type() != Schema.Type.STRUCT) {
                    throw new DataException("Value field '" + valueField + "' must be a struct/object.");
                }
                sourceStruct = root.getStruct(valueField);
                sourceSchema = f.schema();
            }
            if (sourceStruct == null) {
                throw new DataException("Value field '" + valueField + "' is null.");
            }
            namedSchema = renameStruct(sourceSchema, schemaName);
            out = new Struct(namedSchema);
            for (Field field : sourceSchema.fields()) {
                out.put(field.name(), sourceStruct.get(field));
            }

        } else if (value instanceof Map) {
            // --- schemaless path: infer a named Struct from the payload map ---
            @SuppressWarnings("unchecked")
            final Map<String, Object> rootMap = (Map<String, Object>) value;
            final String schemaName = discriminator(rootMap.get(nameField));

            final Object payloadObj = valueField.isEmpty() ? rootMap : rootMap.get(valueField);
            if (payloadObj == null) {
                throw new DataException("Value field '" + valueField + "' is missing or null.");
            }
            if (!(payloadObj instanceof Map)) {
                throw new DataException("Value field '" + valueField + "' must be an object (Map), got "
                    + payloadObj.getClass().getName());
            }
            @SuppressWarnings("unchecked")
            final Map<String, Object> payloadMap = (Map<String, Object>) payloadObj;

            namedSchema = inferStruct(payloadMap, schemaName);
            out = buildStruct(payloadMap, namedSchema);

        } else {
            throw new DataException(
                "SetSchemaNameFromField expects a Struct or Map value, got " + value.getClass().getName()
              + ". Ensure the connector emits structured JSON_SR values.");
        }

        log.info(
            "OUTPUT valueSchema type={}, name={}, valueClass={}",
            namedSchema.type(), 
            namedSchema.name(),
            out != null ? out.getClass().getName() : null);

        log.info("Record value after SMT: {}", out);

        return record.newRecord(
            record.topic(), record.kafkaPartition(),
            record.keySchema(), record.key(),
            namedSchema, out,
            record.timestamp(), record.headers());
    }

    private String discriminator(Object rawName) {
        if (rawName == null) {
            throw new DataException(
                "Discriminator field '" + nameField + "' is missing or null; cannot derive the schema name.");
        }
        final String name = rawName.toString();
        // An empty/whitespace name produces a blank schema name, which
        // TopicRecordNameStrategy rejects ("the message value must only be a record schema").
        if (name.trim().isEmpty()) {
            throw new DataException(
                "Discriminator field '" + nameField + "' is empty; cannot derive the schema name.");
        }
        return name;
    }

    /** Copy a struct schema, changing only its name — field schemas are preserved as-is. */
    private static Schema renameStruct(Schema original, String name) {
        if (original.type() != Schema.Type.STRUCT) {
            throw new DataException("Can only name a STRUCT schema; got " + original.type());
        }
        final SchemaBuilder builder = SchemaBuilder.struct().name(name);
        if (original.isOptional()) {
            builder.optional();
        }
        if (original.defaultValue() != null) {
            builder.defaultValue(original.defaultValue());
        }
        if (original.doc() != null) {
            builder.doc(original.doc());
        }
        if (original.version() != null) {
            builder.version(original.version());
        }
        for (Field field : original.fields()) {
            builder.field(field.name(), field.schema());
        }
        return builder.build();
    }

    /** Infer an (optional) Struct schema from a Map, naming the top-level one. */
    private static Schema inferStruct(Map<String, Object> map, String name) {
        final SchemaBuilder builder = SchemaBuilder.struct().optional();
        if (name != null) {
            builder.name(name);
        }
        for (Map.Entry<String, Object> e : map.entrySet()) {
            builder.field(e.getKey(), inferSchema(e.getValue()));
        }
        return builder.build();
    }

    /** Best-effort Connect schema inference for a JSON-ish value. All schemas are optional. */
    private static Schema inferSchema(Object v) {
        if (v == null) {
            return Schema.OPTIONAL_STRING_SCHEMA; // unknown type; carry as nullable string
        }
        if (v instanceof String) {
            return Schema.OPTIONAL_STRING_SCHEMA;
        }
        if (v instanceof Boolean) {
            return Schema.OPTIONAL_BOOLEAN_SCHEMA;
        }
        if (v instanceof Byte) {
            return Schema.OPTIONAL_INT8_SCHEMA;
        }
        if (v instanceof Short) {
            return Schema.OPTIONAL_INT16_SCHEMA;
        }
        if (v instanceof Integer) {
            return Schema.OPTIONAL_INT32_SCHEMA;
        }
        if (v instanceof Long || v instanceof BigInteger) {
            return Schema.OPTIONAL_INT64_SCHEMA;
        }
        if (v instanceof Float) {
            return Schema.OPTIONAL_FLOAT32_SCHEMA;
        }
        if (v instanceof Double || v instanceof BigDecimal) {
            return Schema.OPTIONAL_FLOAT64_SCHEMA;
        }
        if (v instanceof Map) {
            @SuppressWarnings("unchecked")
            final Map<String, Object> m = (Map<String, Object>) v;
            return inferStruct(m, null); // nested object: unnamed optional struct
        }
        if (v instanceof List) {
            final List<?> list = (List<?>) v;
            Object sample = null;
            for (Object item : list) {
                if (item != null) {
                    sample = item;
                    break;
                }
            }
            final Schema element = sample == null ? Schema.OPTIONAL_STRING_SCHEMA : inferSchema(sample);
            return SchemaBuilder.array(element).optional().build();
        }
        return Schema.OPTIONAL_STRING_SCHEMA; // fallback
    }

    /** Materialize a Struct for the given (inferred) schema from a Map. */
    private static Struct buildStruct(Map<String, Object> map, Schema schema) {
        final Struct struct = new Struct(schema);
        for (Field field : schema.fields()) {
            struct.put(field, convert(map.get(field.name()), field.schema()));
        }
        return struct;
    }

    @SuppressWarnings("unchecked")
    private static Object convert(Object v, Schema schema) {
        if (v == null) {
            return null;
        }
        if (schema.type() == Schema.Type.STRUCT && v instanceof Map) {
            return buildStruct((Map<String, Object>) v, schema);
        }
        if (schema.type() == Schema.Type.ARRAY && v instanceof List) {
            final List<Object> converted = new ArrayList<>();
            for (Object item : (List<?>) v) {
                converted.add(convert(item, schema.valueSchema()));
            }
            return converted;
        }
        return v; // scalar; inferred schema matches the runtime type
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
        // no resources to release
    }
}
