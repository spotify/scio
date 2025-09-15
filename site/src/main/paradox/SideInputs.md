# Side Inputs

Side inputs provide a way to broadcast small amounts of data to all workers.

Side inputs are more performant if they fit entirely into memory.
We therefore recommend using the @ref[singleton variants](SideInputs.md#singleton-variants) if possible, and setting the @ref[--workerCacheMb option](SideInputs.md#workercachemb-option).
For a dataflow job on a standard worker we recommend a maximum size of roughly 1GB for a side input.
If you have a need for a larger side input, see the section on @ref[Sparkey side inputs](extras/Sparkey.md#as-a-side-input).

See also the [Beam Programming Guide's section on Side Inputs](https://beam.apache.org/documentation/programming-guide/#side-inputs) which provides some additional details.

## Standard side inputs

Converting an `SCollection` to a side-input `Seq` or `Iterable` is supported via `asListSideInput` and `asIterableSideInput` respectively:

```scala mdoc:compile-only
import com.spotify.scio.values.{SCollection, SideInput}

val stringElements: SCollection[String] = ???
val stringListSI: SideInput[Seq[String]] = stringElements.asListSideInput
val stringIterSI: SideInput[Iterable[String]] = stringElements.asIterableSideInput
```

For keyed `SCollections`, Scio provides `asMapSideInput` for when there is a unique key-value relationship and `asMultiMapSideInput` for when a key may have multiple values:

```scala mdoc:compile-only
import com.spotify.scio.values.{SCollection, SideInput}

val keyedElements: SCollection[(String, String)] = ???
val mapSingleSI: SideInput[Map[String, String]] = keyedElements.asMapSideInput
val mapMultiSI: SideInput[Map[String, Iterable[String]]] = keyedElements.asMultiMapSideInput
```

## Singleton variants

In addition to standard Beam `SideInput`s, Scio also provides `Singleton` variants that are often more performant than the Beam defaults.

For `SCollection`s with a single element, `asSingletonSideInput` will convert it to a side input:

```scala mdoc:compile-only
import com.spotify.scio.values.{SCollection, SideInput}

val elements: SCollection[Int] = ???
val sumSI: SideInput[Int] = elements.sum.asSingletonSideInput
```

To get an `SideInput` of `Set[T]`, use `asSetSingletonSideInput`:

```scala mdoc:compile-only
import com.spotify.scio.values.{SCollection, SideInput}

val elements: SCollection[String] = ???
val setSI: SideInput[Set[String]] = elements.asSetSingletonSideInput
```

For keyed `SCollection`s, `asMapSingletonSideInput` for when there is a unique key-value relationship and `asMultiMapSingletonSideInput` for when a key may have multiple values:

```scala mdoc:compile-only
import com.spotify.scio.values.{SCollection, SideInput}

val keyedElements: SCollection[(String, String)] = ???
val mapSingleSI: SideInput[Map[String, String]] = keyedElements.asMapSingletonSideInput
val mapMultiSI: SideInput[Map[String, Iterable[String]]] = keyedElements.asMultiMapSingletonSideInput
```

## Side input context

To 'join' a `SideInput`, use `withSideInputs`, then access it via the `SideInputContext`:

```scala
import com.spotify.scio.values.{SCollection, SideInput}

val keyedElements: SCollection[(String, String)] = ???
val mapSingleSI: SideInput[Map[String, String]] = keyedElements.asMapSingletonSideInput

val elements: SCollection[String] = ???
elements
  .withSideInputs(mapSingleSI)
  .map { case (element, ctx) =>
    val mapSingle: Map[String, String] = ctx(mapSingleSI)
    val value: Option[String] = mapSingle.get(element)
    value
  }
```

## workerCacheMb option

By default, Dataflow workers allocate 100MB (see @javadoc[DataflowWorkerHarnessOptions#getWorkerCacheMb](org.apache.beam.runners.dataflow.options.DataflowWorkerHarnessOptions#getWorkerCacheMb--)) of memory for caching side inputs, and falls back to disk or network.
Jobs with large side inputs may therefore be slow.
To override this default, register `DataflowWorkerHarnessOptions` before parsing command line arguments and then pass `--workerCacheMb=N` when submitting the job:

```scala mdoc:compile-only
import com.spotify.scio._
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.runners.dataflow.options.DataflowWorkerHarnessOptions

def main(cmdlineArgs: Array[String]): Unit = {
  PipelineOptionsFactory.register(classOf[DataflowWorkerHarnessOptions])
  val (sc, args) = ContextAndArgs(cmdlineArgs)
  ???
}
```
