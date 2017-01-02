package org.embulk.formatter.avro.converter;

import com.google.common.collect.ImmutableMap;
import org.apache.avro.Schema;

public class AvroValueConverterFactory {
    static public AbstractAvroValueConverter createConverter(Schema.Field field) {
        return detectConverter(field.schema(), field);
    }

    static private AbstractAvroValueConverter detectConverter(Schema schema, Schema.Field field) {
        Schema.Type avroType = schema.getType();
        switch (avroType) {
            case NULL:
                return new AvroNullConverter();
            case BOOLEAN:
                return new AvroBooleanConverter();
            case STRING:
                return new AvroStringConverter();
            case LONG:
                return new AvroLongConverter();
            case DOUBLE:
                return new AvroDoubleConverter();
            case UNION:
                for (Schema s : field.schema().getTypes()) {
                    if (s.getType() != Schema.Type.NULL)
                        return detectConverter(s, field);
                }
                return new AvroNullConverter();
            case ARRAY:
                return new AvroArrayConverter(schema, detectConverter(schema.getElementType(), field));
            case RECORD:
                ImmutableMap.Builder<String, AbstractAvroValueConverter> builder = ImmutableMap.builder();
                for (Schema.Field f : schema.getFields()) {
                    builder.put(f.name(), detectConverter(f.schema(), f));
                }
                return new AvroRecordConverter(schema, builder.build());
            default:
                throw new RuntimeException("Unsupported avro type");
        }
    }
}
