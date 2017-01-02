package org.embulk.formatter.avro.converter;

import org.apache.avro.Schema;

public class AvroBooleanConverter extends AbstractAvroValueConverter {
    public AvroBooleanConverter(Schema schema) {
        super(schema);
    }

    @Override
    public Object booleanColumn(boolean value) {
        throw new RuntimeException("Unsupported typecasting");
    }
}
