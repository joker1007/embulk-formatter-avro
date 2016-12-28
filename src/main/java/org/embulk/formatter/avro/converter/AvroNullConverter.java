package org.embulk.formatter.avro.converter;

import org.msgpack.value.Value;

public class AvroNullConverter extends AbstractAvroValueConverter {
    @Override
    public Object booleanColumn(boolean value) {
        return null;
    }

    @Override
    public Object longColumn(long value) {
        return null;
    }

    @Override
    public Object doubleColumn(double value) {
        return null;
    }

    @Override
    public Object stringColumn(String value) {
        return null;
    }

    @Override
    public Object timestampColumn(String value) {
        return null;
    }

    @Override
    public Object jsonColumn(Value value) {
        return null;
    }
}
