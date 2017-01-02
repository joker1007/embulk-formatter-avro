package org.embulk.formatter.avro.converter;

import org.apache.avro.Schema;
import org.msgpack.value.Value;

public class AvroStringConverter extends AbstractAvroValueConverter {
    public AvroStringConverter(Schema schema) {
        super(schema);
    }

    @Override
    public String booleanColumn(boolean value) {
        return String.valueOf(value);
    }

    @Override
    public String longColumn(long value) {
        return String.valueOf(value);
    }

    @Override
    public String doubleColumn(double value) {
        return String.valueOf(value);
    }

    @Override
    public String stringColumn(String value) {
        return value;
    }

    @Override
    public String timestampColumn(String value) {
        return value;
    }


    @Override
    public String jsonColumn(Value value) {
        return value.toJson();
    }
}
