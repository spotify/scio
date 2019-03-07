# Coder Typeclass

Starting from Scio 0.7.0,

## `Coder` in Apache Beam

As per [Beam's documentation](https://beam.apache.org/documentation/programming-guide/#specifying-coders)

> When Beam runners execute your pipeline, they often need to materialize the intermediate data in your PCollections, which requires converting elements to and from byte strings. The Beam SDKs use objects called Coders to describe how the elements of a given PCollection may be encoded and decoded.

So anytime you create a `SCollection[T]`, Beam needs to know how to go from an instance of `T` to an array of bytes, and from that array of bytes to an intance of `T`.

The Beam SDK has a class called `Coder` that roughly looks like this:

```java
public abstract class Coder<T> implements Serializable {
  public abstract void encode(T value, OutputStream outStream);
  public abstract T decode(InputStream inStream);
}
```

Beam provides built-in Coders for various basic Java types (`Integer`, `Long`, `Double`, etc.). But anytime you create a new class, and that class is used in a `SCollection`, a beam coder needs to be provided.

```scala
case class Foo(x: Int, s: String)

val sc: SCollection[Foo] = ??? // Beam will need an org.apache.beam.sdk.coders.Coder[Foo]
```

## Scio < `0.7.0`

In Scio `0.6.x` and below, Scio would delegate this serialization process to [Kryo](https://github.com/EsotericSoftware/kryo). Kryo's job is to automagically "generate" the serialization logic for any type. The benefit is you don't really have to care about serialization most of the time when writing pipelines with Scio. Using Beam, you would need to explicitly set the coder everytime you use a `PTtransform`.

While it saves a lot of work, it also has a few drawbacks:

- `Kryo` coders can be really inefficient. Especially if you forget to @ref[register your classes using a custom `KryoRegistrar`](../FAQ.md#how-do-i-use-custom-kryo-serializers-).
- The only way to be sure Kryo coders are correctly registered is to write tests and run them with a specific option: (see @ref[kryoRegistrationRequired=true](../FAQ.md#what-kryo-tuning-options-are-there-)).
- Kryo coders are very dynamic and it can be hard to know exactly which coder is used for a given class.
- Kryo coders do not always play well with Beam, and sometime can cause weird runtime exceptions. For example, Beam may sometimes throw an `IllegalMutationException` because of the default Kryo coder implementation.

## Scio >= `0.7.0`

In Scio `0.7.0` and above, the Scala compiler will try to find the correct instance of `Coder` at compile time.
In most cases, the compiler should be able to either directly find a proper `Coder` implementation, or derive one automatically.

Please note that Scio wraps Beam coders in its own `Coder` definition: `com.spotify.scio.coders.Coder`

### Built-in Coder instances

Here's an example REPL session that demonstrate it:

```scala
scala> import com.spotify.scio.coders._
import com.spotify.scio.coders._
scala> Coder[Int] // Try to find a Coder instance for Int
res0: com.spotify.scio.coders.Coder[Int] = <some implementation> // the compiler found a proper instance
```

Here the compiler just found a proper Coder for integers.

Scio also provides Coders for commons collections types:

```scala
scala> Coder[List[String]] // Try to find a Coder instance for List[String]
res1: com.spotify.scio.coders.Coder[List[String]] = <some other implementation>
```

### Automatically derived Coder instances

If you define a case class, the compiler can automatically derive a `Coder` for that class

```scala
scala> case class Demo(i: Int, s: String, xs: List[Double])
defined class Demo
scala> Coder[Demo]
res2: com.spotify.scio.coders.Coder[Demo] = <yet another instance>
```

sealed class hierarchy are also supported:


```scala
scala> :pas
// Entering paste mode (ctrl-D to finish)
sealed trait Top
final case class TA(anInt: Int, aString: String) extends Top
final case class TB(anDouble: Double) extends Top
// Exiting paste mode, now interpreting.

defined trait Top
defined class TA
defined class TB

scala> Coder[Top]
res3: com.spotify.scio.coders.Coder[Top] = <generated instance for Top>
```

### Coder fallbacks

Sometimes, no `Coder` instance can be found, and it's impossible to automatically derive one.
In that case, Scio will fallback to a Kryo coder for that specific type, and if the scalac flag `-Xmacro-settings:show-coder-fallback=true` is set, a warning message will be displayed __at compile time__. This message should help you fix the warning.

```scala
scala> Coder[java.util.Locale]
<console>:15:
 Warning: No implicit Coder found for type, using Kryo fallback:

   >> java.util.Locale


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

res4: com.spotify.scio.coders.Coder[java.util.Locale] = Fallback(java.util.Locale)
```

Here for example, the compiler could not find a proper instance of `Coder[Locale]`, and suggest you implement one yourself.

Note that this message is not limited to direct invocation of fallback. For example, if you declare a case class that uses `Locale` internally, the compiler will show the same warning:


```scala
scala> case class Demo2(i: Int, s: String, xs: List[java.util.Locale])

defined class Demo2

scala> Coder[Demo2]
<console>:17:
 Warning: No implicit Coder found for type, using Kryo fallback:

   >> java.util.Locale


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

res5: com.spotify.scio.coders.Coder[Demo2] = Transform(Record([Lscala.Tuple2;@60736dd9),<function1>)
```

Scio will still use a "proper" Coder for `Int`, `String` and `List`. Only the serialization of `Locale` instances is delegated to Kryo.

## Upgrating to `v0.7.0` or above: Migrating to static coder

Migrating to Scio `0.7.x` from an older version is likely to break a few things at compile time in your project.
See the complete @ref[v0.7.0 Migration Guide](../migrations/v0.7.0-Migration-Guide.md) for more information.
