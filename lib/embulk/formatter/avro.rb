Embulk::JavaPlugin.register_formatter(
  "avro", "org.embulk.formatter.avro.AvroFormatterPlugin",
  File.expand_path('../../../../classpath', __FILE__))
