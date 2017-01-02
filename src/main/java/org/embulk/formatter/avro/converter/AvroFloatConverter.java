package org.embulk.formatter.avro.converter;

public class AvroFloatConverter extends AbstractAvroValueConverter {
    @Override
    public Float longColumn(long value) {
        return Long.valueOf(value).floatValue();
    }

    @Override
    public Float doubleColumn(double value) {
        return Double.valueOf(value).floatValue();
    }

    @Override
    public Float stringColumn(String value) {
        return Float.parseFloat(value);
    }

    @Override
    public Float timestampColumn(String value) {
        return Float.parseFloat(value);
    }
}
