# Voyager

Scio supports Spotify's [Voyager](https://github.com/spotify/voyager), which provides an easy to use API on top of `hnswlib` that that
works in python and java.

## Write

A keyed `SCollection` with `String` keys and `Array[Float]` vector values can be saved with @scaladoc[asVoyager](com.spotify.scio.extra.voyager.VoyagerPairSCollection#asVoyager(path:String,spaceType:com.spotify.voyager.jni.Index.SpaceType,storageDataType:com.spotify.voyager.jni.Index.StorageDataType,dim:Int,ef:Long,m:Long)):

```scala
import com.spotify.scio.values.SCollection
import com.spotify.scio.extra.voyager._
import com.spotify.voyager.jni.Index.{SpaceType, StorageDataType}

val dim: Int = ???
val ef: Long = ???
val m: Int = ???
val storageType: StorageDataType = ???
val spaceType: SpaceType = ???
val itemVectors: SCollection[(String, Array[Float])] = ???
itemVectors.asVoyager("gs://output-path", spaceType, storageType, dim, ef, m)
```

## Side Input

A Voyager index can be read directly as a `SideInput` with @scaladoc[voyagerSideInput](com.spotify.scio.extra.voyager.VoyagerScioContext#voyagerSideInput(com.spotify.voyager.jni.Index.SpaceType,storageDataType:com.spotify.voyager.jni.Index.StorageDataType,dim:Int)):

```scala
import com.spotify.scio._
import com.spotify.scio.values.SCollection
import com.spotify.scio.extra.voyager._
import com.spotify.voyager.jni.Index.{SpaceType, StorageDataType}

val sc: ScioContext = ???

val dim: Int = ???
val storageType: StorageDataType = ???
val spaceType: SpaceType = ???
val itemVectors: SCollection[(String, Array[Float])] = ???
val voyagerSI: SideInput[VoyagerReader] = sc.voyagerSideInput("gs://input-path", spaceType, storageType, dim)
```

Alternatively, an `SCollection` can be converted directly to a `SideInput` with @scaladoc[asVoyagerSideInput](com.spotify.scio.extra.voyager.VoyagerPairSCollection#asVoyagerSideInput(spaceType:com.spotify.voyager.jni.Index.SpaceType,storageDataType:com.spotify.voyager.jni.Index.StorageDataType,dim:Int,ef:Long,m:Long)):
```scala
import com.spotify.scio.values.SCollection
import com.spotify.scio.extra.voyager._
import com.spotify.voyager.jni.Index.{SpaceType, StorageDataType}

val dim: Int = ???
val ef: Long = ???
val m: Int = ???
val storageType: StorageDataType = ???
val spaceType: SpaceType = ???
val itemVectors: SCollection[(String, Array[Float])] = ???
val voyagerSI: SideInput[VoyagerReader] = itemVectors.asVoyagerSideInput(spaceType, storageType, dim, ef, m)
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
