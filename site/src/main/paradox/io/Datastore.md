# Datastore

Scio supports [Google Datastore](https://cloud.google.com/datastore) via Beam's @javadoc[DatastoreIO](org.apache.beam.sdk.io.gcp.datastore.DatastoreIO).

[Magnolify's](https://github.com/spotify/magnolify) `EntityType` (available as part of the `magnolify-datastore` artifact) provides automatically-derived mappings between Datastore's `Entity` and scala case classes. See [full documentation here](https://github.com/spotify/magnolify/blob/main/docs/datastore.md) and [an example usage here](https://spotify.github.io/scio/examples/MagnolifyDatastoreExample.scala.html).

## Reads

Read an `SCollection` of `com.google.datastore.v1.Entity` from Datastore with @scaladoc[datastore](com.spotify.scio.datastore.syntax.ScioContextOps#datastore(projectId:String,query:com.google.datastore.v1.Query,namespace:String):com.spotify.scio.values.SCollection[com.google.datastore.v1.Entity]):

```scala mdoc:compile-only
import com.spotify.scio._
import com.spotify.scio.values.SCollection
import com.spotify.scio.datastore._
import com.google.datastore.v1.{Entity, Query}

val sc: ScioContext = ???

val projectId: String = ???
val query: Query = Query.getDefaultInstance
val entities: SCollection[Entity] = sc.datastore(projectId, query)
```

## Writes

Write a collection of 

@scaladoc[saveAsDatastore](com.spotify.scio.datastore.syntax.SCollectionEntityOps#saveAsDatastore(projectId:String):com.spotify.scio.io.ClosedTap[Nothing])

```scala mdoc:compile-only
import com.spotify.scio._
import com.spotify.scio.values.SCollection
import com.spotify.scio.datastore._
import com.google.datastore.v1.{Entity, Query}

val projectId: String = ???
val entities: SCollection[Entity] = ???
entities.saveAsDatastore(projectId)
```
