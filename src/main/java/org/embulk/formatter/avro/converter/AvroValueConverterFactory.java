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
                return new AvroNullConverter();
            case BOOLEAN:
                return new AvroBooleanConverter();
            case STRING:
                return new AvroStringConverter();
            case INT:
                return new AvroIntConverter();
            case LONG:
                return new AvroLongConverter();
            case FLOAT:
                return new AvroFloatConverter();
            case DOUBLE:
                return new AvroDoubleConverter();
            case UNION:
                for (Schema s : schema.getTypes()) {
                    if (s.getType() != Schema.Type.NULL)
                        return detectConverter(s);
                }
                return new AvroNullConverter();
            case ARRAY:
                return new AvroArrayConverter(schema, detectConverter(schema.getElementType()));
            case MAP:
                return new AvroMapConverter(detectConverter(schema.getValueType()));
            case RECORD:
                ImmutableMap.Builder<String, AbstractAvroValueConverter> builder = ImmutableMap.builder();
                for (Schema.Field f : schema.getFields()) {
                    builder.put(f.name(), detectConverter(f.schema()));
                }
                return new AvroRecordConverter(schema, builder.build());
            default:
                throw new RuntimeException("Unsupported avro type");
        }
    }
}
