package org.embulk.formatter.avro.converter;

import avro.shaded.com.google.common.collect.ImmutableMap;
import org.apache.avro.Schema;
import org.msgpack.value.Value;

import java.util.Map;

public class AvroMapConverter extends AbstractAvroValueConverter {
    private AbstractAvroValueConverter elementConverter;

    public AvroMapConverter(Schema schema, AbstractAvroValueConverter elementConverter) {
        super(schema);
        this.elementConverter = elementConverter;
    }

    @Override
    public Map<String, Object> jsonColumn(Value value) {
        if (!value.isMapValue())
            throw new RuntimeException("Support only map type json record");

        Map<Value, Value> map = value.asMapValue().map();

        ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();

        for (Map.Entry<Value, Value> entry : map.entrySet()) {
            switch (entry.getValue().getValueType()) {
                case STRING:
                    builder.put(entry.getKey().toString(), elementConverter.stringColumn(entry.getValue().asStringValue().toString()));
                    break;
                case INTEGER:
                    builder.put(entry.getKey().toString(), elementConverter.longColumn(entry.getValue().asIntegerValue().toLong()));
                    break;
                case FLOAT:
                    builder.put(entry.getKey().toString(), elementConverter.doubleColumn(entry.getValue().asFloatValue().toDouble()));
                    break;
                case BOOLEAN:
                    builder.put(entry.getKey().toString(), elementConverter.booleanColumn(entry.getValue().asBooleanValue().getBoolean()));
                    break;
                case ARRAY:
                    builder.put(entry.getKey().toString(), elementConverter.jsonColumn(entry.getValue().asArrayValue()));
                    break;
                case MAP:
                    builder.put(entry.getKey().toString(), elementConverter.jsonColumn(entry.getValue().asMapValue()));
                    break;
                default:
                    throw new RuntimeException("Irregular Messagepack type");
            }
        }
        return builder.build();
    }
}
