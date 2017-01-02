package org.embulk.formatter.avro.converter;

import org.apache.avro.Schema;

public class AvroIntConverter extends AbstractAvroValueConverter {
    public AvroIntConverter(Schema schema) {
        super(schema);
    }

    @Override
    public Integer longColumn(long value) {
        return Long.valueOf(value).intValue();
    }

    @Override
    public Integer doubleColumn(double value) {
        return Double.valueOf(value).intValue();
    }

    @Override
    public Integer stringColumn(String value) {
        return Integer.parseInt(value);
    }

    @Override
    public Integer timestampColumn(String value) {
        return Integer.parseInt(value);
    }
}
