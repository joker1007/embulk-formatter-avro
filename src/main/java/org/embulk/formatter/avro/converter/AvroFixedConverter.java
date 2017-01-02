package org.embulk.formatter.avro.converter;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.GenericFixed;

import java.util.List;

public class AvroFixedConverter extends AbstractAvroValueConverter {
    private Schema avroSchema;

    AvroFixedConverter(Schema schema) {
        this.avroSchema = schema;
    }

    @Override
    public GenericFixed stringColumn(String value) {
        return new GenericData.Fixed(avroSchema, value.getBytes());
    }
}
