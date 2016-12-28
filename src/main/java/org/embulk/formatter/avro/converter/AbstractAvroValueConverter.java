package org.embulk.formatter.avro.converter;

import org.msgpack.value.Value;

abstract public class AbstractAvroValueConverter {
    public Object booleanColumn(boolean value) {
        throw new RuntimeException("Unsupported typecasting");
    }

    public Object longColumn(long value) {
        throw new RuntimeException("Unsupported typecasting");
    }

    public Object doubleColumn(double value) {
        throw new RuntimeException("Unsupported typecasting");
    }

    public Object stringColumn(String value) {
        throw new RuntimeException("Unsupported typecasting");
    }

    public Object timestampColumn(String value) {
        throw new RuntimeException("Unsupported typecasting");
    }

    public Object jsonColumn(Value value) {
        throw new RuntimeException("Unsupported typecasting");
    }
}
