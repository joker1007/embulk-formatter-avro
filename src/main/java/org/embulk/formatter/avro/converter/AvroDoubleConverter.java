package org.embulk.formatter.avro.converter;

import org.apache.avro.Schema;

public class AvroDoubleConverter extends AbstractAvroValueConverter {
    public AvroDoubleConverter(Schema schema) {
        super(schema);
    }

    @Override
    public Double longColumn(long value) {
        return Long.valueOf(value).doubleValue();
    }

    @Override
    public Double doubleColumn(double value) {
        return value;
    }

    @Override
    public Double stringColumn(String value) {
        return Double.parseDouble(value);
    }

    @Override
    public Double timestampColumn(String value) {
        return Double.parseDouble(value);
    }
}
