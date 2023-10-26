# Voyager

Scio supports Spotify's [Voyager](https://github.com/spotify/voyager), which provides an easy to use API on top of `hnswlib` that that
works in python and java.

## Write

A keyed `SCollection` with `String` keys and `Array[Float]` vector values can be saved with @scaladoc[asVoyager](com.spotify.scio.extra.voyager.syntax.VoyagerPairSCollectionOps#asVoyager(uri:com.spotify.scio.extra.voyager.VoyagerUri,spaceType:com.spotify.voyager.jni.Index.SpaceType,storageDataType:com.spotify.voyager.jni.Index.StorageDataType,dim:Int,ef:Long,m:Long):com.spotify.scio.values.SCollection[com.spotify.scio.extra.voyager.VoyagerUri]):

```scala
import com.spotify.scio.values.SCollection
import com.spotify.scio.extra.voyager._
import com.spotify.voyager.jni.Index.{SpaceType, StorageDataType}

val voyagerUri = VoyagerUri("gs://output-path")
val space: SpaceType = ???
val numDimensions: Int = ???
val itemVectors: SCollection[(String, Array[Float])] = ???
itemVectors.asVoyager(uri, space, numDimensions)
```

## Side Input

A Voyager index can be read directly as a `SideInput` with @scaladoc[asVoyagerSideInput](com.spotify.scio.extra.voyager.syntax.VoyagerScioContextOps#voyagerSideInput(uri:com.spotify.scio.extra.voyager.VoyagerUri,spaceType:com.spotify.voyager.jni.Index.SpaceType,storageDataType:com.spotify.voyager.jni.Index.StorageDataType,dim:Int):com.spotify.scio.values.SideInput[com.spotify.scio.extra.voyager.VoyagerReader]):

```scala
import com.spotify.scio._
import com.spotify.scio.values.SCollection
import com.spotify.scio.extra.voyager._
import com.spotify.voyager.jni.Index.{SpaceType, StorageDataType}

val sc: ScioContext = ???

val voyagerUri = VoyagerUri("gs://output-path")

// index saved with voyager v1 requires settings
val space: SpaceType = ???
val numDimensions: Int = ???
val storageDataType: StorageDataType = ???
val voyagerV1: SideInput[VoyagerReader] = sc.voyagerSideInput(voyagerUri, space, numDimensions, storageDataType)
// index saved with voyager v2 extracts settings from its metadata
val voyagerV2: SideInput[VoyagerReader] = sc.voyagerSideInput(voyagerUri)
```

Alternatively, an `SCollection` can be converted directly to a `SideInput` with @scaladoc[asVoyagerSideInput](com.spotify.scio.extra.voyager.syntax.VoyagerSCollectionOps#asVoyagerSideInput(spaceType:com.spotify.voyager.jni.Index.SpaceType,storageType:com.spotify.voyager.jni.Index.StorageDataType,dim:Int):com.spotify.scio.values.SideInput[com.spotify.scio.extra.voyager.VoyagerReader]):
```scala
import com.spotify.scio.values.SCollection
import com.spotify.scio.extra.voyager._
import com.spotify.voyager.jni.Index.{SpaceType, StorageDataType}

val space: SpaceType = ???
val numDimensions: Int = ???
val itemVectors: SCollection[(String, Array[Float])] = ???
val voyagerSI: SideInput[VoyagerReader] = itemVectors.asVoyagerSideInput(space, numDimensions)
```

An @scaladoc[VoyagerReader](com.spotify.scio.extra.voyager.VoyagerReader) provides access to querying the Voyager index to get their nearest neighbors.
```scala
import com.spotify.scio.values.{SCollection, SideInput}
import com.spotify.scio.extra.voyager._

val voyagerSI: SideInput[VoyagerReader] = ???
val elements: SCollection[(String, Array[Float])] = ???
val maxNumResults: Int = ???
val ef: Int = ???

val queryResults: SCollection[(String, Array[VoyagerResult])] = elements
  .withSideInputs(voyagerSI)
  .map { case ((label, vector), ctx) =>
    val voyagerReader: VoyagerReader = ctx(voyagerSI)
    (label, voyagerReader.getNearest(vector, maxNumResults, ef))
  }
```
