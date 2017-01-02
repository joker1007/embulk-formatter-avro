package org.embulk.formatter.avro;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigInject;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.formatter.avro.converter.AbstractAvroValueConverter;
import org.embulk.formatter.avro.converter.AvroValueConverterFactory;
import org.embulk.spi.BufferAllocator;
import org.embulk.spi.Column;
import org.embulk.spi.FileOutput;
import org.embulk.spi.FormatterPlugin;
import org.embulk.spi.Page;
import org.embulk.spi.PageOutput;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.time.TimestampFormatter;
import org.embulk.spi.unit.LocalFile;
import org.embulk.spi.util.FileOutputOutputStream;
import org.embulk.spi.util.Timestamps;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class AvroFormatterPlugin
        implements FormatterPlugin
{
    public interface PluginTask
            extends Task, TimestampFormatter.Task
    {
        // configuration option 1 (required integer)
        @Config("avsc")
        LocalFile getAvsc();

        @Config("column_options")
        @ConfigDefault("{}")
        Map<String, TimestampFormatter.TimestampColumnOption> getColumnOptions();

        @ConfigInject
        public BufferAllocator getBufferAllocator();
    }

    @Override
    public void transaction(ConfigSource config, Schema schema,
            FormatterPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        // validate avsc option
        try {
            File avsc = task.getAvsc().getFile();
            new org.apache.avro.Schema.Parser().parse(avsc);
        } catch (IOException e) {
            throw new ConfigException("avsc file is not found");
        }


        // validate column_options
        for (String columnName : task.getColumnOptions().keySet()) {
            schema.lookupColumn(columnName);  // throws SchemaConfigException
        }

        control.run(task.dump());
    }

    @Override
    public PageOutput open(TaskSource taskSource, final Schema schema,
            FileOutput output)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);

        final TimestampFormatter[] timestampFormatters = Timestamps.newTimestampColumnFormatters(task, schema, task.getColumnOptions());
        final FileOutputOutputStream stream = new FileOutputOutputStream(output, task.getBufferAllocator(), FileOutputOutputStream.CloseMode.CLOSE);

        final org.apache.avro.Schema avroSchema;
        final DataFileWriter<GenericRecord> writer;
        try {
            File avsc = task.getAvsc().getFile();
            avroSchema = new org.apache.avro.Schema.Parser().parse(avsc);
            GenericDatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(avroSchema);
            writer = new DataFileWriter<>(datumWriter);
            stream.nextFile();
            writer.create(avroSchema, stream);
        } catch (IOException e) {
            throw new ConfigException("avsc file is not found");
        }

        final AbstractAvroValueConverter[] avroValueConverters = new AbstractAvroValueConverter[schema.size()];
        List<AbstractAvroValueConverter> array = new ArrayList<>();
        for (Column c : schema.getColumns()) {
            org.apache.avro.Schema.Field field = avroSchema.getField(c.getName());
            if (field != null) {
                avroValueConverters[c.getIndex()] = AvroValueConverterFactory.createConverter(field);
            }
        }

        return new PageOutput() {
            private final PageReader pageReader = new PageReader(schema);

            @Override
            public void add(Page page) {
                pageReader.setPage(page);

                while (pageReader.nextRecord()) {
                    GenericRecord record = new GenericData.Record(avroSchema);

                    schema.visitColumns(new AvroFormatterColumnVisitor(pageReader, timestampFormatters, avroValueConverters, record));

                    try {
                        writer.append(record);
                    } catch (IOException e) {
                        e.printStackTrace();
                        throw new RuntimeException("failed to write");
                    }
                }

                try {
                    writer.flush();
                } catch (IOException e) {
                    e.printStackTrace();
                    throw new RuntimeException("failed to write");
                }
            }

            @Override
            public void finish() {
                try {
                    writer.flush();
                } catch (IOException e) {
                    e.printStackTrace();
                    throw new RuntimeException("failed to write");
                }
                stream.finish();
            }

            @Override
            public void close() {
                try {
                    writer.close();
                } catch (IOException e) {
                    e.printStackTrace();
                    throw new RuntimeException("failed to write");
                }
                stream.close();
            }
        };
    }
}
