package org.embulk.formatter.avro.converter;

import org.apache.avro.Schema;
import org.msgpack.value.Value;

import java.util.HashMap;
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

        Map<String, Object> converted = new HashMap();

        for (Map.Entry<Value, Value> entry : map.entrySet()) {
            switch (entry.getValue().getValueType()) {
                case STRING:
                    converted.put(entry.getKey().toString(), elementConverter.stringColumn(entry.getValue().asStringValue().toString()));
                    break;
                case INTEGER:
                    converted.put(entry.getKey().toString(), elementConverter.longColumn(entry.getValue().asIntegerValue().toLong()));
                    break;
                case FLOAT:
                    converted.put(entry.getKey().toString(), elementConverter.doubleColumn(entry.getValue().asFloatValue().toDouble()));
                    break;
                case BOOLEAN:
                    converted.put(entry.getKey().toString(), elementConverter.booleanColumn(entry.getValue().asBooleanValue().getBoolean()));
                    break;
                case ARRAY:
                    converted.put(entry.getKey().toString(), elementConverter.jsonColumn(entry.getValue().asArrayValue()));
                    break;
                case MAP:
                    converted.put(entry.getKey().toString(), elementConverter.jsonColumn(entry.getValue().asMapValue()));
                    break;
                default:
                    throw new RuntimeException("Irregular Messagepack type");
            }
        }
        return converted;
    }
}
