# Sorter

The @scaladoc[sortValues](com.spotify.scio.extra.sorter.syntax.SorterOps#sortValues(memoryMB:Int)(implicitk1Coder:com.spotify.scio.coders.Coder[K1],implicitk2Coder:com.spotify.scio.coders.Coder[K2],implicitvCoder:com.spotify.scio.coders.Coder[V]):com.spotify.scio.values.SCollection[(K1,Iterable[(K2,V)])]) transform sorts values by a secondary key following a `groupByKey` on the primary key, spilling sorting to disk if required.
The `memoryMB` controls the allowable in-memory overhead before the sorter spills data to disk.
Keys are compared based on the byte-array representations produced by their Beam coder.

```scala
import com.spotify.scio.values.SCollection
import com.spotify.scio.extra.sorter._

val elements: SCollection[(String, (String, Int))] = ???
val sorted: SCollection[(String, Iterable[(String, Int)])] = elements
  .groupByKey
  .sortValues(memoryMB = 100)
```
