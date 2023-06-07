# Annoy

Scio supports Spotify's [Annoy](https://github.com/spotify/annoy), an approximate nearest neighbors library, via [annoy-java](https://github.com/spotify/annoy-java) and [annoy4s](https://github.com/annoy4s/annoy4s).

## Write

A keyed `SCollection` with `Int` keys and `Array[Float]` values

@scaladoc[`asAnnoy`(com.spotify.scio.extra.annoy.AnnoyPairSCollection#asAnnoy(path:String,metric:com.spotify.scio.extra.annoy.package.AnnoyMetric,dim:Int,nTrees:Int):com.spotify.scio.values.SCollection[com.spotify.scio.extra.annoy.AnnoyUri])

```scala
import com.spotify.scio.values.SCollection
import com.spotify.scio.extra.annoy._

val itemVectors: SCollection[(Int, Array[Float])] = ???
```


## Side Input

```scala


```


TODO
