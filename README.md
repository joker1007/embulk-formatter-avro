# Avro formatter plugin for Embulk

[Avro](http://avro.apache.org/) formatter plugin for Embulk.

## Overview

* **Plugin type**: formatter

## Support avro types

Support all avro basic types.

- string
- int
- long
- float
- double
- boolean
- enum
- fixed
- array
- map
- record

But typecasting is restricted by embulk column type.
See. [AvroValueConverters](https://github.com/joker1007/embulk-formatter-avro/tree/master/src/main/java/org/embulk/formatter/avro/converter),

## Configuration

- **avsc**: avro schema (avsc) filepath (string, required)
- **codec**: avro codec type (enum: `deflate`, `bzip2`, `xz`, `snappy`, optional)
- **compression\_level**: avro codec compression level (integer, optional, for only `deflate` and `xz` codec)
- **skip\_error\_record**: If you want to skip error record, set true (boolean, default: `false`)

## Example

```yaml
out:
  type: file
  path_prefix: ./out_
  file_ext: avro
  formatter:
    type: avro
    avsc: schema.avsc
    skip_error_record: true
```


## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```
