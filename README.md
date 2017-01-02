# Avro formatter plugin for Embulk

[Avro](http://avro.apache.org/) formatter plugin for Embulk.

## Overview

* **Plugin type**: formatter

## Configuration

- **avsc**: avro schema (avsc) filepath (string, required)
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
