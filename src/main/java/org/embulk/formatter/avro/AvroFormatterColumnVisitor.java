package org.embulk.formatter.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.embulk.formatter.avro.converter.AbstractAvroValueConverter;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.PageReader;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.time.TimestampFormatter;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.Value;

public class AvroFormatterColumnVisitor implements ColumnVisitor {
    private PageReader pageReader;
    private TimestampFormatter[] timestampFormatters;
    private AbstractAvroValueConverter[] avroValueConverters;
    private GenericRecord record;
    private org.apache.avro.Schema avroSchema;

    AvroFormatterColumnVisitor(PageReader pageReader, TimestampFormatter[] timestampFormatters, AbstractAvroValueConverter[] avroValueConverters, GenericRecord record) {
        this.pageReader = pageReader;
        this.timestampFormatters = timestampFormatters;
        this.avroValueConverters = avroValueConverters;
        this.record = record;
    }

    @Override
    public void booleanColumn(Column column) {
        if (pageReader.isNull(column))
            return;
        AbstractAvroValueConverter converter = avroValueConverters[column.getIndex()];
        if (converter == null)
            return;
        Boolean value = pageReader.getBoolean(column);
        Object result = converter.booleanColumn(value);
        record.put(column.getName(), result);
    }

    @Override
    public void longColumn(Column column) {
        if (pageReader.isNull(column))
            return;
        AbstractAvroValueConverter converter = avroValueConverters[column.getIndex()];
        if (converter == null)
            return;
        Long value = pageReader.getLong(column);
        Object result = converter.longColumn(value);
        record.put(column.getName(), result);
    }

    @Override
    public void doubleColumn(Column column) {
        if (pageReader.isNull(column))
            return;
        AbstractAvroValueConverter converter = avroValueConverters[column.getIndex()];
        if (converter == null)
            return;
        Double value = pageReader.getDouble(column);
        Object result = converter.doubleColumn(value);
        record.put(column.getName(), result);
    }

    @Override
    public void stringColumn(Column column) {
        if (pageReader.isNull(column))
            return;
        AbstractAvroValueConverter converter = avroValueConverters[column.getIndex()];
        if (converter == null)
            return;
        String value = pageReader.getString(column);
        Object result = converter.stringColumn(value);
        record.put(column.getName(), result);
    }

    @Override
    public void timestampColumn(Column column) {
        if (pageReader.isNull(column))
            return;
        AbstractAvroValueConverter converter = avroValueConverters[column.getIndex()];
        if (converter == null)
            return;
        Timestamp value = pageReader.getTimestamp(column);
        String formatted = timestampFormatters[column.getIndex()].format(value);
        Object result = converter.timestampColumn(formatted);
        record.put(column.getName(), result);
    }

    @Override
    public void jsonColumn(Column column) {
        if (pageReader.isNull(column))
            return;
        AbstractAvroValueConverter converter = avroValueConverters[column.getIndex()];
        if (converter == null)
            return;
        Value value = pageReader.getJson(column);
        Object result = converter.jsonColumn(value);
        record.put(column.getName(), result);
    }
}
