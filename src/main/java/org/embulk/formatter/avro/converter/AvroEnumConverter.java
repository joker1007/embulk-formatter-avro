package org.embulk.formatter.avro.converter;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericEnumSymbol;

import java.util.List;

public class AvroEnumConverter extends AbstractAvroValueConverter {
    private List<String> enumSymbols;

    AvroEnumConverter(Schema schema, List<String> enumSymbols) {
        super(schema);
        this.enumSymbols = enumSymbols;
    }

    @Override
    public GenericEnumSymbol stringColumn(String value) {
        if (enumSymbols.contains(value)) {
            return new GenericData.EnumSymbol(avroSchema, value);
        } else {
            throw new RuntimeException(String.format("%s is not in %s", value, enumSymbols.toString()));
        }
    }
}
