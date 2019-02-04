# Avro

## Read Avro files

Scio comes with support for reading Avro files. Avro supports generic or specific records, Scio supports both via the same method (`avroFile`), but depending on the type parameter.

### Specific record:

```
# SpecificRecordClass is compiled from Avro schema files
sc.avroFile[SpecificRecordClass]("gs://path-to-data/lake/part-*.avro")
  .map(record => ???)
# `record` is of your SpecificRecordClass type
```

### Generic record:

```
import org.apache.avro.generic.GenericRecord
sc.avroFile[GenericRecord]("gs://path-to-data/lake/part-*.avro", yourAvroSchema)
  .map(record => ???)
# `record` is of GenericRecord type
```

## Write Avro files

Scio comes with support for writing Avro files. Avro supports generic or specific records, Scio supports both via the same method (`saveAsAvroFile`), but depending on the type of the content of `SCollection`.

### Specific record:

```
# type of Avro specific records will hold information about schema,
# therefor Scio will figure out the schema by itself
sc.map(<build Avro specific records>)
  .saveAsAvroFile("gs://path-to-data/lake/output")
```

### Generic record:

```
# writing Avro generic records requires additional argument `schema`
sc.map(<build Avro generic records>)
  .saveAsAvroFile("gs://path-to-data/lake/output", schema = yourAvroSchema)
```

## Rules for schema evolution

* Unless impossible, provide default values for your fields.
* New field must have a default value.
* You can only delete field which has default value.
* Do not change data type of an existing fields. If needed add a new field to the schema.
* Do not rename existing fields. If needed use aliases.

## Common issues/guidelines

* Follow Avro [guidelines](https://avro.apache.org/docs/current/spec.html), especially the one about [schema evolution](#rules-for-schema-evolution)
* Wherever possible use specific records
* Use `Builder` pattern to construct Avro records
