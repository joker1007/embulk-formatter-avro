package org.embulk.formatter.avro.converter;

public class AvroIntConverter extends AbstractAvroValueConverter {
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
