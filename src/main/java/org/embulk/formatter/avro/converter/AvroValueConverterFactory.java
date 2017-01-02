package org.embulk.formatter.avro.converter;

import com.google.common.collect.ImmutableMap;
import org.apache.avro.Schema;

public class AvroValueConverterFactory {
    static public AbstractAvroValueConverter createConverter(Schema.Field field) {
        return detectConverter(field.schema());
    }

    static private AbstractAvroValueConverter detectConverter(Schema schema) {
        Schema.Type avroType = schema.getType();
        switch (avroType) {
            case NULL:
                return new AvroNullConverter(schema);
            case BOOLEAN:
                return new AvroBooleanConverter(schema);
            case STRING:
                return new AvroStringConverter(schema);
            case INT:
                return new AvroIntConverter(schema);
            case LONG:
                return new AvroLongConverter(schema);
            case FLOAT:
                return new AvroFloatConverter(schema);
            case DOUBLE:
                return new AvroDoubleConverter(schema);
            case ENUM:
                return new AvroEnumConverter(schema, schema.getEnumSymbols());
            case FIXED:
                return new AvroFixedConverter(schema);
            case UNION:
                for (Schema s : schema.getTypes()) {
                    if (s.getType() != Schema.Type.NULL)
                        return detectConverter(s);
                }
                return new AvroNullConverter(schema);
            case ARRAY:
                return new AvroArrayConverter(schema, detectConverter(schema.getElementType()));
            case MAP:
                return new AvroMapConverter(schema, detectConverter(schema.getValueType()));
            case RECORD:
                ImmutableMap.Builder<String, AbstractAvroValueConverter> builder = ImmutableMap.builder();
                for (Schema.Field f : schema.getFields()) {
                    builder.put(f.name(), detectConverter(f.schema()));
                }
                return new AvroRecordConverter(schema, builder.build());
            default:
                throw new RuntimeException(String.format("%s of %s is unsupported avro type", schema.getType(), schema.getName()));
        }
    }
}
