# Kryo

Scio uses a framework called [Kryo](https://github.com/EsotericSoftware/kryo) to serialize objects that need to be shuffled between workers. Network throughput can easily become a bottleneck for your pipeline, so optimizing serialization is an easy win. If you use [Dataflow's shuffler service](https://cloud.google.com/blog/big-data/2017/06/introducing-cloud-dataflow-shuffle-for-up-to-5x-performance-improvement-in-data-analytic-pipelines), you pay per GB shuffled so you can save money even if shuffling is not a bottleneck.

By registering your classes at compile time, Kryo can serialize far more efficiently that doing it on the fly. The default serializer includes the full classpath of each class you serialize. By pre-registering the classes you want to serialize, Kryo can replace this with an int as identifier. For some pipelines we observed a 30-40% reduction of bytes shuffled.

## How to enable KryoRegistrar
Add the following class. You can rename it, but its name has to end in `KryoRegistrar`. Also make sure that the [Macro Paradise](https://docs.scala-lang.org/overviews/macros/paradise.html) plugin is enabled for your project.

````scala
import com.spotify.scio.coders.KryoRegistrar
import com.twitter.chill._

@KryoRegistrar
class MyKryoRegistrar extends IKryoRegistrar {
  override def apply(k: Kryo): Unit = {
    // Take care of common Scala classes; tuples, Enumerations, ...
    val reg = new AllScalaRegistrar
    reg(k)

    k.registerClasses(List(
      // All classes that might be shuffled, e.g.:
      classOf[foo.bar.MyClass],

      // Class that takes type parameters:
      classOf[_root_.java.util.ArrayList[_]],
      // But you can also explicitly do:
      classOf[Array[Byte]],

      // Private class; cannot use classOf:
      Class.forName("com.spotify.scio.extra.sparkey.LocalSparkeyUri"),

      // Some common Scala objects
      None.getClass,
      Nil.getClass
    ))
  }
}
````

_Note:_ since Dataflow may shuffle data at any point, you not only have to include classes that are explicitly shuffled (through join or groupBy), but also those returned by map, flatMap, etc.

## Verifying it works
You can add the following class to your test folder; it will enforce registration of classes during your tests. It only works if you actually run your job in tests, so be sure to include a `JobTest` or so for each pipeline you run.

````scala
import com.esotericsoftware.kryo.Kryo
import com.spotify.scio.coders.KryoRegistrar
import com.twitter.chill.IKryoRegistrar

/** Makes sure we don't forget to register encoders, enabled only in tests not to crash production. */
@KryoRegistrar
class TestKryoRegistrar extends IKryoRegistrar {
  def apply(k: Kryo): Unit = {
    k.setRegistrationRequired(true)
  }
}
````

If you missed registering any classes, you'll get an error that looks like this:

```bash
[info]   java.lang.IllegalArgumentException: Class is not registered: org.apache.avro.generic.GenericData$Record
[info] Note: To register this class use: kryo.register(org.apache.avro.generic.GenericData$Record.class);
```
Which you solve by adding `classOf[GenericData.Record]` or `Class.forName("org.apache.avro.generic.GenericData$Record")` in `MyKryoRegistrar`.
