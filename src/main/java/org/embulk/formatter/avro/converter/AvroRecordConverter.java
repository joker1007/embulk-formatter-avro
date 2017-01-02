package org.embulk.formatter.avro.converter;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;

import java.util.Map;

public class AvroRecordConverter extends AbstractAvroValueConverter {
    private Map<String, AbstractAvroValueConverter> converterTable;

    public AvroRecordConverter(Schema schema, Map<String, AbstractAvroValueConverter> converterTable) {
        super(schema);
        this.converterTable = converterTable;
    }

    @Override
    public GenericRecord jsonColumn(Value value) {
        if (!value.isMapValue())
            throw new RuntimeException("Support only map type json record");

        Map<Value, Value> map = value.asMapValue().map();

        GenericRecord record = new GenericData.Record(avroSchema);
        for (Map.Entry<String, AbstractAvroValueConverter> entry : converterTable.entrySet()) {
            Value key = ValueFactory.newString(entry.getKey());
            if (!map.containsKey(key)) {
                record.put(entry.getKey(), null);
            } else {
                Value child = map.get(ValueFactory.newString(entry.getKey()));
                switch (child.getValueType()) {
                    case STRING:
                        record.put(entry.getKey(), entry.getValue().stringColumn(child.asStringValue().toString()));
                        break;
                    case INTEGER:
                        record.put(entry.getKey(), entry.getValue().longColumn(child.asIntegerValue().toLong()));
                        break;
                    case FLOAT:
                        record.put(entry.getKey(), entry.getValue().doubleColumn(child.asFloatValue().toDouble()));
                        break;
                    case BOOLEAN:
                        record.put(entry.getKey(), entry.getValue().booleanColumn(child.asBooleanValue().getBoolean()));
                        break;
                    case ARRAY:
                        record.put(entry.getKey(), entry.getValue().jsonColumn(child.asArrayValue()));
                        break;
                    case MAP:
                        record.put(entry.getKey(), entry.getValue().jsonColumn(child.asMapValue()));
                        break;
                    default:
                        throw new RuntimeException("Irregular Messagepack type");
                }
            }
        }
        return record;
    }
}
