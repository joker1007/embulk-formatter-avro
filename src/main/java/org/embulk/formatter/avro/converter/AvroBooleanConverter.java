package org.embulk.formatter.avro.converter;

public class AvroBooleanConverter extends AbstractAvroValueConverter {
    @Override
    public Object booleanColumn(boolean value) {
        throw new RuntimeException("Unsupported typecasting");
    }
}
