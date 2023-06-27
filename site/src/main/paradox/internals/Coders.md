# Coder Typeclass

## `Coder` in Apache Beam

As per [Beam's documentation](https://beam.apache.org/documentation/programming-guide/#specifying-coders)

> When Beam runners execute your pipeline, they often need to materialize the intermediate data in your PCollections, which requires converting elements to and from byte strings. The Beam SDKs use objects called Coders to describe how the elements of a given PCollection may be encoded and decoded.

For the most part, coders are used when Beam transfer intermediate data between workers over the network. They may also be used by beam to test instances for equality.
Anytime you create an `SCollection[T]`, Beam needs to know how to go from an instance of `T` to an array of bytes, and from that array of bytes to an instance of `T`.

The Beam SDK defines a class called `Coder` that roughly looks like this:

```java
public abstract class Coder<T> implements Serializable {
  public abstract void encode(T value, OutputStream outStream);
  public abstract T decode(InputStream inStream);
}
```

Beam provides built-in Coders for various basic Java types (`Integer`, `Long`, `Double`, etc.). But anytime you create a new class, and that class is used in an `SCollection`, a beam coder needs to be provided.

```scala mdoc:silent
import com.spotify.scio.values.SCollection

case class Foo(x: Int, s: String)
def coll: SCollection[Foo] = ??? // Beam will need an org.apache.beam.sdk.coders.Coder[Foo]
```

### When are `Coder` used ?

#### Shuffling data

Whenever intermediate data is shuffled, Beam will need to serialize and deserialize that data to transfer it between workers. In Scio any `*byKey` (`groupByKey`, `reduceByKey`, etc.) transform will trigger a shuffle.

#### Cluster scaling up and down

When the runner scales up and down your cluster size, data needs to be redistributed between workers. Beam therefore needs to transfer data over the network, which means serializing and deserializing it by using `Coder`.

#### GroupByKey

Grouping by key uses `Coder` for two reasons: First, as we have already seen, GBK triggers a shuffle, and therefore go through a serialization / deserialization cycle.

Second, grouping elements by key means that Beam needs to compare them and be able to decide whether two instances are equal. In Beam, and in the context of a `groupByKey` (or any `*byKey` operation), the equality of keys is tested by comparing their serialized form.

Let's say we have defined a class `Identifier` and we use it as the key in a `groupByKey` transform:

```scala
import com.spotify.scio.values.SCollection

val coll: SCollection[(Identifier, Foo)] = ???
val grouped: SCollection[(Identifier, Iterable[Foo])] = coll.groupByKey()
```

To decide whether two instances of `Identifier` `id1` and `id2` are equal, Beam will compare them after serialization. For example:

```scala
// (pseudo-code)
coder.encode(id1) // 00010011
coder.encode(id2) // 00010010
// -> i1 and i2 are NOT equal
```

**When they are used to test equality, coders are required to be deterministic.**
If a non-deterministic `Coder` is used to test equality, an exception is thrown:

```scala mdoc:reset:invisible
import com.spotify.scio.ScioContext
val sc = ScioContext.forTest()
```

```scala mdoc:crash
val grouped =
  sc.parallelize(List((1.2, "foo"), (42.5, "bar"), (1.2, "baz")))
    .groupByKey
sc.run()
```

## Scio `0.6.x` and below

In Scio `0.6.x` and below, Scio would delegate this serialization process to [Kryo](https://github.com/EsotericSoftware/kryo). Kryo's job is to automagically "generate" the serialization logic for any type. The benefit is you don't really have to care about serialization most of the time when writing pipelines with Scio. Using Beam, you would need to explicitly set the coder every time you use a `PTtransform`.

While it saves a lot of work, it also has a few drawbacks:

- `Kryo` coders can be really inefficient. Especially if you forget to @ref:[register your classes using a custom `KryoRegistrar`](../FAQ.md#how-do-i-use-custom-kryo-serializers-).
- The only way to be sure Kryo coders are correctly registered is to write tests and run them with a specific option: (see @ref:[kryoRegistrationRequired=true](../FAQ.md#what-kryo-tuning-options-are-there-)).
- Kryo coders are very dynamic and it can be hard to know exactly which coder is used for a given class.
- Kryo coders do not always play well with Beam, and sometime can cause weird runtime exceptions. For example, Beam may sometimes throw an `IllegalMutationException` because of the default Kryo coder implementation.

## Scio `0.7.0` and above

In Scio `0.7.0` and above, the Scala compiler will try to find the correct instance of `Coder` at compile time. In most cases, the compiler should be able to either directly find a proper `Coder` implementation, or derive one automatically.

### Scio `Coder` vs Beam `Coder`

Both Scio and Beam define a class called `Coder`. For the most part when writing a job, you will be interacting with `com.spotify.scio.coders.Coder`.

Scio `Coder` and its implementations simply form an [ADT](https://en.wikipedia.org/wiki/Algebraic_data_type) where each implementation is a building block that covers one of the possible cases:

- `Beam`: a simple wrapper around a Beam Coder
- `Disjunction`: Represent a Coder that makes a choice between different possible implementations. It is for example used to serialize ADTs and `Either`
- `Record`: A Coder for record-like structures like case classes and tuples.
- `Transform`: A Coder implemented by "transforming" another Coder.
- `Fallback`: A default `Coder`. Used when there is no better option.

There is also a "special" coder called `KVCoder`. It is a specific coder for Key-Value pairs. Internally Beam treats @javadoc[KV](org.apache.beam.sdk.values.KV) differently from other types so Scio needs to do the same.

It is important to note that **Scio's coders are only representations** of those cases but **do not actually implement any serialization logic**. Before the job starts, those coders will be *materialized*, meaning they will be converted to instances of @javadoc[org.apache.beam.sdk.coders.Coder](org.apache.beam.sdk.coders.Coder).
Thanks to this technique, Scio can dynamically change the behavior of coders depending on the execution context. For example coders may handle nullable values differently depending on options passed to the job.

@javadoc[org.apache.beam.sdk.coders.Coder](org.apache.beam.sdk.coders.Coder) instances on the other hand are the actual implementations of serialization and deserialization logic. Among other thing, each instance of `org.apache.beam.sdk.coders.Coder[T]` defines two methods:

```scala
class ExampleCoder extends org.apache.beam.sdk.coders.Coder[Example] {
  def decode(inStream: InputStream): Example = ???
  def encode(value: Example, outStream: OutputStream): Unit = ???
}
```

### How Scio picks a Coder instance

Every method in Scio that may potentially need to serialize and deserialize data takes an [implicit](https://docs.scala-lang.org/tour/implicit-parameters.html) `Coder` argument. See for example the definition of `SCollection.map`:

```scala
def map[U: Coder](f: T => U): SCollection[U] = // implementation
//           ↑
//    Implicit Coder[U] lookup
```

This type signature means the following: The method `map` (defined in `SCollection[T]`), applied to a function from `T` to `U`, will return an `SCollection[U]`. On top of that this method has a *context bound* `U: Coder`, meaning a `Coder[U]` needs to be available in the implicit context.

So **at compile time** the Scala compiler will try to find an appropriate `Coder`. If it fails to find one, the compilation will fail.

When the compiler looks for an implicit for a concrete type `Foo`, three cases can happen:

#### There exist an `Coder[Foo]` in scope

Scio comes with a number of implementation for common types like primitives (`Int`, `Float`, `String`, etc.), common Scala and Java types (`Option`, `Either`, `List`, etc.) and some types from the Beam API. In that case the available `Coder` will simply be used. It is also possible for the user (aka you) to provide an implementation. Here's an example REPL session that demonstrate it:

```scala mdoc
import com.spotify.scio.coders._
Coder[Int] // Try to find a Coder instance for Int
```

Here the compiler just found a proper Coder for integers.
Scio also provides Coders for commons collections types:

```scala mdoc
Coder[List[String]] // Try to find a Coder instance for List[String]
```

#### No `Coder[Foo]` is available but the compiler can derive one

For certain type (for example case classes with a public constructor), Scio can derive an inline `Coder` implementation **at compile time**. Note that **it does not generate source code**.

```scala mdoc
case class Demo(i: Int, s: String, xs: List[Double])
Coder[Demo]
```

sealed class hierarchy are also supported:

```scala mdoc
sealed trait Top
final case class TA(anInt: Int, aString: String) extends Top
final case class TB(anDouble: Double) extends Top
Coder[Top]
```

#### No `Coder[Foo]` is available and the compiler can not derive one

Sometimes, no `Coder` instance can be found, and it's impossible to automatically derive one.
In that case, Scio will fallback to a `Kryo` coder for that specific type. Note that **it might negatively impact the performance of your job**.

If the scalac flag `-Xmacro-settings:show-coder-fallback=true` is set, a warning message will be displayed **at compile time**. This message should help you fix the warning.

While compiling the following example with `-Xmacro-settings:show-coder-fallback=true`

```scala mdoc:reset
import com.spotify.scio.coders._
val localeCoder = Coder[java.util.Locale]
```

Scalac will output:

```
Warning: No implicit Coder found for the following type:

  >> java.util.Locale

using Kryo fallback instead.


  Scio will use a fallback Kryo coder instead.

  If a type is not supported, consider implementing your own implicit Coder for this type.
  It is recommended to declare this Coder in your class companion object:

      object Locale {
        import com.spotify.scio.coders.Coder
        import org.apache.beam.sdk.coders.AtomicCoder

        implicit def coderLocale: Coder[Locale] =
          Coder.beam(new AtomicCoder[Locale] {
            def decode(in: InputStream): Locale = ???
            def encode(ts: Locale, out: OutputStream): Unit = ???
          })
      }

  If you do want to use a Kryo coder, be explicit about it:

      implicit def coderLocale: Coder[Locale] = Coder.kryo[Locale]

  Additional info at:
  - https://spotify.github.io/scio/internals/Coders
```

In this example, the compiler could not find a proper instance of `Coder[Locale]`, and suggest you implement one yourself.

Note that this message is not limited to direct invocation of fallback. For example, if you declare a case class that uses `Locale` internally, the compiler will show the same warning:

```scala mdoc:reset
import com.spotify.scio.coders._
case class Demo2(i: Int, s: String, xs: List[java.util.Locale])
val demoCoder = Coder[Demo2]
```

`Int`, `String` and `List` all have predefined `Coder` instances but `Locale` does not. The serialization of `Locale` instances is delegated to Kryo.

## Compiler flags and warnings

When Scio automatically derives a Coder for a given type, it may issue warnings about potential performance issues. For example, the default implementation of `Coder[GenericRecord]` is very inefficient and Scio will issue a message if it is used:

```
[info] Using a fallback coder for Avro's GenericRecord is discouraged as it is VERY inefficient.
[info] It is highly recommended to define a proper Coder[GenericRecord] using:
[info]
[info]   Coder.avroGenericRecordCoder(schema)
```

It is also possible to pass a flag to the compiler to issue a message anytime the fallback coder is used:

```
[info]  Warning: No implicit Coder found for the following type:
[info]
[info]    >> com.google.common.collect.SetMultimap[String,String]
[info]
[info]  using Kryo fallback instead.
```

To activate this feature, pass `-Xmacro-settings:show-coder-fallback=true` to `scalac` in your build file:

```scala
scalacOptions += "-Xmacro-settings:show-coder-fallback=true"
```

## How to build a custom Coder

It is possible for the user to define their own `Coder` implementation. Scio provides @scaladoc[builder functions](com.spotify.scio.coders.CoderGrammar) in the `Coder` object. If you want to create a custom `Coder`, you should use one of the those three builder:

- **`Coder.beam`**: Create a Scio `Coder` that simply wraps a Beam implementation. For example:
```scala mdoc
import com.spotify.scio.coders._
import org.apache.beam.sdk.coders.DoubleCoder
implicit def doubleCoder =
  Coder.beam(DoubleCoder.of())
```
- **`Coder.transform`**: Create a Coder for a type `B` by transforming the Beam implementation for a type `A`. Usually useful for `Coder` that depend on another `Coder`:
```scala mdoc
import java.io.{InputStream, OutputStream}
import org.apache.beam.sdk.coders.AtomicCoder
class ListCoder[T](bc: org.apache.beam.sdk.coders.Coder[T]) extends AtomicCoder[List[T]] {
  override def encode(value: List[T], outStream: OutputStream): Unit = ???
  override def decode(inStream: InputStream): List[T] = ???
}
implicit def listCoder[T: Coder]: Coder[List[T]] = Coder.transform(Coder[T])(bc => Coder.beam(new ListCoder[T](bc)))
```
- **`Coder.xmap`**: Create a Coder for a type `B` by reusing a `Coder[A]`. `xmap` simply apply the provided function to convert `B` to `A` and back. See for example a possible `Coder[Char]` based on an existing `Coder[Byte]`
```scala mdoc
implicit def charCoder: Coder[Char] = Coder.xmap(Coder[Byte])(_.toChar, _.toByte)
```

### ⚠ Serialization ⚠

Coder instances **have to** be `Serializable`. You do not need to extend `Serializable` explicitly since the `Coder` trait already does, but you do need to make sure that your implementation is not referencing a non-serializable object in any way.

Note that in test mode (when you use `JobTest`), Scio will make sure that all the coders used in the job are serializable.

### Testing custom coders

Scio provides a few assertions specific to coders. See @scaladoc[CoderAssertions](com.spotify.scio.testing.CoderAssertions$).

## Null values support

By default, and for performance reasons, Scio coders will expect the values to serialized to never be `null`.

This may cause the following exception to be thrown:

```
org.apache.beam.sdk.Pipeline$PipelineExecutionException: java.lang.RuntimeException:
  Exception while trying to `encode` an instance of scala.Tuple3:
  Can't encode field _3 value null
```

There are 2 ways to fix this issue:

  1. **Recommended**: Replace `null` values by `Option` in you job code.
  2. **NOT recommended**: pass the following flag when you start the job:

     ```
     --nullableCoders=true
     ```

     If you pass this option, Scio will assume that every value are potentially `null`. This include every single fields in your case classes and each every elements in collections. **It introduces overhead and may slow down your job execution.**

## Upgrading to `v0.7.0` or above: Migrating to static coder

Migrating to Scio `0.7.x` from an older version is likely to break a few things at compile time in your project.
See the complete @ref:[v0.7.0 Migration Guide](../releases/migrations/v0.7.0-Migration-Guide.md) for more information.
