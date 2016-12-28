package org.embulk.formatter.avro.converter;

public class AvroLongConverter extends AbstractAvroValueConverter {
    @Override
    public Long longColumn(long value) {
        return value;
    }

    @Override
    public Long doubleColumn(double value) {
        return Math.round(value);
    }

    @Override
    public Long stringColumn(String value) {
        return Long.parseLong(value);
    }

    @Override
    public Long timestampColumn(String value) {
        return Long.parseLong(value);
    }
}
