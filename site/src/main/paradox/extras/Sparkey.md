# Sparkey

Scio supports Spotify's [Sparkey](https://github.com/spotify/sparkey), which provides a simple disk-backed key-value store.

At Spotify, sparkeys are typically used in pipelines as side-inputs where the size of the side-input would be too large to reasonably fit into memory but can still fit on disk.
Scio's suite of `largeHash` functions are backed by sparkeys.

Scio supports writing any type with a coder to a sparkey by first converting 

## As a Side-Input

A sparkey side-input is a good choice when you have a very large dataset that needs to be joined with a relatively small dataset, but one which is still too large to fit into memory.
In this case, the @scaladoc[asSparkeySideInput](com.spotify.scio.extra.sparkey.SparkeyPairSCollection#asSparkeySideInput(implicitw:com.spotify.scio.extra.sparkey.package.SparkeyWritable[K,V]):com.spotify.scio.values.SideInput[com.spotify.sparkey.SparkeyReader]) method can be used to broadcast the smaller dataset to all workers and avoid shuffle.

```scala mdoc:compile-only
import com.spotify.scio.values.{SCollection, SideInput}
import com.spotify.scio.extra.sparkey._
import com.spotify.sparkey._

case class Track(title: String, artistId: String)
case class ArtistMetadata(artistId: String, name: String)

val tracks: SCollection[Track] = ???
val metadata: SCollection[ArtistMetadata] = ???

val artistNameSI: SideInput[SparkeyReader] = metadata
  .map { am => am.artistId -> am.name }
  .asSparkeySideInput

tracks.withSideInputs(artistNameSI)
  .map { case (track, context) =>
    val optArtistName = context(artistNameSI).get(track.artistId)
    track -> optArtistName
  }
```

See also @ref[Large Hash Joins](../Joins.md#large-hash-join), which do the same thing as this simple example but with a more compact syntax.

## Writing

If a sparkey can be reused by multiple pipelines, it can be saved permanently with @scaladoc[asSparkey](com.spotify.scio.extra.sparkey.SparkeyPairSCollection#asSparkey(implicitw:com.spotify.scio.extra.sparkey.package.SparkeyWritable[K,V]):com.spotify.scio.values.SCollection[com.spotify.scio.extra.sparkey.SparkeyUri])

```scala mdoc:compile-only
import com.spotify.scio.values.SCollection
import com.spotify.scio.extra.sparkey._

val elements: SCollection[(String, String)] = ???
elements.asSparkey("gs://output-path")
```

## Reading

Previously-written sparkeys can be loaded directly as side-inputs:

```scala mdoc:compile-only
import com.spotify.scio.ScioContext
import com.spotify.scio.values.{SCollection, SideInput}
import com.spotify.scio.extra.sparkey._
import com.spotify.sparkey._

val sc: ScioContext = ???
val sparkeySI: SideInput[SparkeyReader] = sc.sparkeySideInput("gs://input-path")
```
