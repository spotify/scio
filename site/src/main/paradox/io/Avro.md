# Avro

## Read Avro files

Scio comes with support for reading Avro files. Avro supports generic or specific records, Scio supports both via the same method (`avroFile`), but depending on the type parameter.

### Read Specific records

```scala mdoc:reset:silent
import com.spotify.scio.ScioContext
import com.spotify.scio.values.SCollection
import com.spotify.scio.avro._

import org.apache.avro.specific.SpecificRecord

def sc: ScioContext = ???

// SpecificRecordClass is compiled from Avro schema files
def result: SCollection[SpecificRecord] = sc.avroFile[SpecificRecord]("gs://path-to-data/lake/part-*.avro")
```

### Read Generic records

```scala mdoc:reset:silent
import com.spotify.scio.ScioContext
import com.spotify.scio.values.SCollection
import com.spotify.scio.avro._

import org.apache.avro.generic.GenericRecord
import org.apache.avro.Schema

def yourAvroSchema: Schema = ???

def sc: ScioContext = ???

def result: SCollection[GenericRecord] = sc.avroFile("gs://path-to-data/lake/part-*.avro", yourAvroSchema)
```

### Read Typed Case Classes
Scio uses [magnolify-avro](https://github.com/spotify/magnolify/blob/master/docs/avro.md) to derive an Avro schema for a given case class at compile type, similar to how @ref:[coders](../internals/Coders.md) work. See this [mapping table](https://github.com/spotify/magnolify/blob/master/docs/mapping.md) for how Scala and Avro types map.

```scala mdoc:reset:silent
import com.spotify.scio.ScioContext
import com.spotify.scio.values.SCollection
import com.spotify.scio.avro._

case class Foo(x: Int, s: String)

def sc: ScioContext = ???

def result: SCollection[Foo] = sc.typedAvroFile[Foo]("gs://path-to-data/lake/part-*.avro")
```

## Write Avro files

Scio comes with support for writing Avro files. Avro supports generic or specific records, Scio supports both via the same method (`saveAsAvroFile`), but depending on the type of the content of `SCollection`.

### Write Specific records

```scala mdoc:reset:silent
import com.spotify.scio.values.SCollection
import com.spotify.scio.avro._

import org.apache.avro.specific.SpecificRecord

case class Foo(x: Int, s: String)
def sc: SCollection[Foo] = ???

// convert to avro SpecificRecord
def fn(f: Foo): SpecificRecord = ???

// type of Avro specific records will hold information about schema,
// therefor Scio will figure out the schema by itself

def result = sc.map(fn).saveAsAvroFile("gs://path-to-data/lake/output")
```

### Write Generic records

```scala mdoc:reset:silent
import com.spotify.scio.values.SCollection
import com.spotify.scio.avro._

import org.apache.avro.generic.GenericRecord
import org.apache.avro.Schema

case class Foo(x: Int, s: String)
def sc: SCollection[Foo] = ???

def yourAvroSchema: Schema = ???

// convert to avro SpecificRecord
def fn(f: Foo): GenericRecord = ???

// writing Avro generic records requires additional argument `schema`
def result = sc.map(fn).saveAsAvroFile("gs://path-to-data/lake/output", schema = yourAvroSchema)
```

### Write Typed Case Classes

When writing case classes as Avro files, the schema is derived from the case class and all fields are written.

```scala mdoc:reset:silent
import com.spotify.scio.values.SCollection
import com.spotify.scio.avro._


case class Foo(x: Int, s: String)
def foo: SCollection[Foo] = ???

def result = foo.saveAsTypedAvroFile("gs://path-to-data/lake/output")
```

## Rules for schema evolution

* Unless impossible, provide default values for your fields.
* New field must have a default value.
* You can only delete field which has default value.
* Do not change data type of an existing fields. If needed add a new field to the schema.
* Do not rename existing fields. If needed use aliases.

## Common issues/guidelines

* Follow Avro [guidelines](https://avro.apache.org/docs/current/spec.html), especially the one about [schema evolution](http://avro.apache.org/docs/current/spec.html#Schema+Resolution)
* Wherever possible use specific records
* Use `Builder` pattern to construct Avro records
