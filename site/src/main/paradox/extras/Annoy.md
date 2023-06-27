# Annoy

Scio integrates with Spotify's [Annoy](https://github.com/spotify/annoy), an approximate nearest neighbors library, via [annoy-java](https://github.com/spotify/annoy-java) and [annoy4s](https://github.com/annoy4s/annoy4s).

## Write

A keyed `SCollection` with `Int` keys and `Array[Float]` vector values can be saved with @scaladoc[asAnnoy](com.spotify.scio.extra.annoy.AnnoyPairSCollection#asAnnoy(path:String,metric:com.spotify.scio.extra.annoy.package.AnnoyMetric,dim:Int,nTrees:Int):com.spotify.scio.values.SCollection[com.spotify.scio.extra.annoy.AnnoyUri]):

```scala
import com.spotify.scio.values.SCollection
import com.spotify.scio.extra.annoy._

val metric: AnnoyMetric = ???
val numDimensions: Int = ???
val numTrees: Int = ???
val itemVectors: SCollection[(Int, Array[Float])] = ???
itemVectors.asAnnoy("gs://output-path", metric, numDimensions, numTrees)
```

## Side Input

An Annoy file can be read directly as a `SideInput` with @scaladoc[annoySideInput](com.spotify.scio.extra.annoy.AnnoyScioContext#annoySideInput(path:String,metric:com.spotify.scio.extra.annoy.package.AnnoyMetric,dim:Int):com.spotify.scio.values.SideInput[com.spotify.scio.extra.annoy.package.AnnoyReader]):

```scala
import com.spotify.scio._
import com.spotify.scio.values.SideInput
import com.spotify.scio.extra.annoy._

val sc: ScioContext = ???

val metric: AnnoyMetric = ???
val numDimensions: Int = ???
val annoySI: SideInput[AnnoyReader] = sc.annoySideInput("gs://input-path", metric, numDimensions)
```

Alternatively, an `SCollection` can be converted directly to a `SideInput` with @scaladoc
[`asAnnoySideInput`](com.spotify.scio.extra.annoy.AnnoyPairSCollection#asAnnoySideInput(metric:com.spotify.scio.extra.annoy.package.AnnoyMetric,dim:Int):com.spotify.scio.values.SideInput[com.spotify.scio.extra.annoy.package.AnnoyReader]):

```scala
import com.spotify.scio.values.{SCollection, SideInput}
import com.spotify.scio.extra.annoy._

val metric: AnnoyMetric = ???
val numDimensions: Int = ???
val numTrees: Int = ???
val itemVectors: SCollection[(Int, Array[Float])] = ???
val annoySI: SideInput[AnnoyReader] = itemVectors.asAnnoySideInput(metric, numDimensions, numTrees)
```

An @scaladoc[AnnoyReader](com.spotify.scio.extra.annoy.AnnoyReader) provides access to item vectors and their nearest neighbors:

```scala
import com.spotify.scio.values.{SCollection, SideInput}
import com.spotify.scio.extra.annoy._

val annoySI: SideInput[AnnoyReader] = ???
val elements: SCollection[Int] = ???
elements
  .withSideInputs(annoySI)
  .map { case (element, ctx) =>
    val annoyReader: AnnoyReader = ctx(annoySI)
    val vec: Array[Float] = annoyReader.getItemVector(element)
    element -> annoyReader.getNearest(vec, 1)
  }
```
