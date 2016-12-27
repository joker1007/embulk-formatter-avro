package org.embulk.formatter.avro;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.PageReader;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.time.TimestampFormatter;

public class AvroFormatterColumnVisitor implements ColumnVisitor {
    private PageReader pageReader;
    private TimestampFormatter[] timestampFormatters;
    private GenericRecord record;
    private org.apache.avro.Schema avroSchema;

    AvroFormatterColumnVisitor(PageReader pageReader, TimestampFormatter[] timestampFormatters, org.apache.avro.Schema avroSchema, GenericRecord record) {
        this.pageReader = pageReader;
        this.timestampFormatters = timestampFormatters;
        this.avroSchema = avroSchema;
        this.record = record;
    }

    @Override
    public void booleanColumn(Column column) {
        if (pageReader.isNull(column))
            return;
        Boolean value = pageReader.getBoolean(column);
        record.put(column.getName(), value);
    }

    @Override
    public void longColumn(Column column) {
        if (pageReader.isNull(column))
            return;
        Long value = pageReader.getLong(column);
        record.put(column.getName(), value);
    }

    @Override
    public void doubleColumn(Column column) {
        if (pageReader.isNull(column))
            return;
        Double value = pageReader.getDouble(column);
        record.put(column.getName(), value);
    }

    @Override
    public void stringColumn(Column column) {
        if (pageReader.isNull(column))
            return;
        String value = pageReader.getString(column);
        System.out.println(value);
        record.put(column.getName(), value);
    }

    @Override
    public void timestampColumn(Column column) {
        if (pageReader.isNull(column))
            return;
        Timestamp value = pageReader.getTimestamp(column);
        String formatted = timestampFormatters[column.getIndex()].format(value);
        org.apache.avro.Schema.Type avroType = avroSchema.getField(column.getName()).schema().getType();
        switch (avroType) {
            case STRING:
                record.put(column.getName(), formatted);
                break;
            case INT:
                record.put(column.getName(), Integer.parseInt(formatted));
            case LONG:
                record.put(column.getName(), Long.parseLong(formatted));
            case FLOAT:
            case DOUBLE:
                record.put(column.getName(), Double.parseDouble(formatted));
            default:
                throw new RuntimeException("Unsupported type");
        }
    }

    @Override
    public void jsonColumn(Column column) {
    }
}
