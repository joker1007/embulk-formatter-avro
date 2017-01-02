package org.embulk.formatter.avro.converter;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;

public class AvroFixedConverter extends AbstractAvroValueConverter {
    AvroFixedConverter(Schema schema) {
        super(schema);
    }

    @Override
    public GenericFixed stringColumn(String value) {
        return new GenericData.Fixed(avroSchema, value.getBytes());
    }
}
