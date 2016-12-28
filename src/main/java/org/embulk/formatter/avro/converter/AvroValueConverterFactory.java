package org.embulk.formatter.avro.converter;

import org.apache.avro.Schema;

public class AvroValueConverterFactory {
    static public AbstractAvroValueConverter createConverter(Schema.Field field) {
        return detectConverter(field.schema().getType(), field);
    }

    static private AbstractAvroValueConverter detectConverter(Schema.Type avroType, Schema.Field field) {
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
                        return detectConverter(s.getType(), field);
                }
                return new AvroNullConverter();
            case RECORD:
            default:
                throw new RuntimeException("Unsupported avro type");
        }
    }
}
