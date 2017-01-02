package org.embulk.formatter.avro.converter;

import org.apache.avro.Schema;
import org.msgpack.value.Value;

abstract public class AbstractAvroValueConverter {
    protected Schema avroSchema;

    public AbstractAvroValueConverter(Schema schema) {
        this.avroSchema = schema;
    }

    public Object booleanColumn(boolean value) {
        throw new RuntimeException(String.format("%s cannot convert to %s of %s field", value, avroSchema.getType(), avroSchema.getName()));
    }

    public Object longColumn(long value) {
        throw new RuntimeException(String.format("%s cannot convert to %s of %s field", value, avroSchema.getType(), avroSchema.getName()));
    }

    public Object doubleColumn(double value) {
        throw new RuntimeException(String.format("%s cannot convert to %s of %s field", value, avroSchema.getType(), avroSchema.getName()));
    }

    public Object stringColumn(String value) {
        throw new RuntimeException(String.format("%s cannot convert to %s of %s field", value, avroSchema.getType(), avroSchema.getName()));
    }

    public Object timestampColumn(String value) {
        throw new RuntimeException(String.format("%s cannot convert to %s of %s field", value, avroSchema.getType(), avroSchema.getName()));
    }

    public Object jsonColumn(Value value) {
        throw new RuntimeException(String.format("%s cannot convert to %s of %s field", value, avroSchema.getType(), avroSchema.getName()));
    }
}
