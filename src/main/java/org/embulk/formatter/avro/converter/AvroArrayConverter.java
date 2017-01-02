package org.embulk.formatter.avro.converter;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.msgpack.value.Value;

import java.util.List;

public class AvroArrayConverter extends AbstractAvroValueConverter {
    private AbstractAvroValueConverter elementConverter;

    public AvroArrayConverter(Schema schema, AbstractAvroValueConverter elementConverter) {
        super(schema);
        this.elementConverter = elementConverter;
    }

    @Override
    public GenericArray<Object> jsonColumn(Value value) {
        if (!value.isArrayValue())
            throw new RuntimeException("Support only array type json record");

        List<Value> list = value.asArrayValue().list();

        GenericArray<Object> array = new GenericData.Array<>(list.size(), avroSchema);
        for (Value val : list) {
            switch (val.getValueType()) {
                case STRING:
                    array.add(elementConverter.stringColumn(val.asStringValue().toString()));
                    break;
                case INTEGER:
                    array.add(elementConverter.longColumn(val.asIntegerValue().toLong()));
                    break;
                case FLOAT:
                    array.add(elementConverter.doubleColumn(val.asFloatValue().toDouble()));
                    break;
                case BOOLEAN:
                    array.add(elementConverter.booleanColumn(val.asBooleanValue().getBoolean()));
                    break;
                case ARRAY:
                    array.add(elementConverter.jsonColumn(val.asArrayValue()));
                    break;
                case MAP:
                    array.add(elementConverter.jsonColumn(val.asMapValue()));
                    break;
                default:
                    throw new RuntimeException("Irregular Messagepack type");
            }
        }
        return array;
    }
}
