package org.embulk.formatter.avro.converter;

import org.apache.avro.Schema;
import org.msgpack.value.Value;

public class AvroBooleanConverter extends AbstractAvroValueConverter {
    public AvroBooleanConverter(Schema schema) {
        super(schema);
    }

    @Override
    public Boolean booleanColumn(boolean value) {
        return value;
    }

    @Override
    public Boolean longColumn(long value) {
        return value != 0;
    }

    @Override
    public Boolean doubleColumn(double value) {
        return value != 0;
    }

    @Override
    public Boolean stringColumn(String value) {
        return Boolean.valueOf(value);
    }
}
