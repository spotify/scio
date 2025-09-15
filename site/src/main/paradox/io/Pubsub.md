# PubSub

Scio supports [Google Cloud PubSub](https://cloud.google.com/pubsub/docs/overview)

## Read from PubSub

Use the appropriate `PubsubIO` method with `ScioContext.read` to read into strings, avro, protobuf, beam's `PubsubMessage`, or into any type supported by a scio `Coder`. Pass a `PubsubIO.ReadParam` to configure whether reading from a topic or subscription.

```scala mdoc:compile-only
import com.spotify.scio._
import com.spotify.scio.values._
import com.spotify.scio.pubsub._

val a: PubsubIO[String] = PubsubIO.string("strings")

import com.spotify.scio.avro.Account
val b: PubsubIO[Account] = PubsubIO.avro[Account]("avros")

import com.spotify.scio.proto.Track.TrackPB
val c: PubsubIO[TrackPB] = PubsubIO.proto[TrackPB]("protos")

import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage
val d: PubsubIO[PubsubMessage] = PubsubIO.pubsub[PubsubMessage]("messages")

case class MyClass(s: String, i: Int)
val e: PubsubIO[MyClass] = PubsubIO.coder[MyClass]("myclasses")

val sc: ScioContext = ???

// read strings from a subscription
val in1: SCollection[String] = sc.read(a)(PubsubIO.ReadParam(PubsubIO.Subscription))

// or from a topic
val in2: SCollection[String] = sc.read(a)(PubsubIO.ReadParam(PubsubIO.Topic))
```

The `withAttributes` methods give access to the PubSub attributes within the SCollection:

```scala mdoc:compile-only
import com.spotify.scio._
import com.spotify.scio.values._
import com.spotify.scio.pubsub._

val sc: ScioContext = ???
val in: SCollection[(String, Map[String, String])] =
  sc.read(PubsubIO.withAttributes[String]("strings"))(PubsubIO.ReadParam(PubsubIO.Subscription))
    .map { case (element, attributes) =>
      attributes.get("name")
      ???
    }
```

## Write to PubSub

PubSub write methods use the same PubSubIO methods as reading:

```scala mdoc:compile-only
import com.spotify.scio._
import com.spotify.scio.values._
import com.spotify.scio.pubsub._

val strings: SCollection[String] = ???
strings.write(PubsubIO.string("strings"))(PubsubIO.WriteParam())

import com.spotify.scio.avro.Account
val accounts: SCollection[Account] = ???
accounts.write(PubsubIO.avro[Account]("accounts"))(PubsubIO.WriteParam())

import com.spotify.scio.proto.Track.TrackPB
val tracks: SCollection[TrackPB] = ???
tracks.write(PubsubIO.proto[TrackPB]("tracks"))(PubsubIO.WriteParam())

import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage
val messages: SCollection[PubsubMessage] = ???
messages.write(PubsubIO.pubsub[PubsubMessage]("messages"))(PubsubIO.WriteParam())

case class MyClass(s: String, i: Int)
val myClasses: SCollection[MyClass] = ???
myClasses.write(PubsubIO.coder[MyClass]("myClasses"))(PubsubIO.WriteParam())
```

Writing attributes:

```scala mdoc:compile-only
import com.spotify.scio._
import com.spotify.scio.values._
import com.spotify.scio.pubsub._

val strings: SCollection[(String, Map[String, String])] = ???
strings.write(PubsubIO.withAttributes[String]("strings"))(PubsubIO.WriteParam())
```
