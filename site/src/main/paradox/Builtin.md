# Built-in Functionality

## ContextAndArgs

## Options

## Use native Beam functionality

- `ScioContext#customInput` to apply a `PTransform[_ >: PBegin, PCollection[T]]` (source) and get a `SCollection[T]`.
- `SCollection#applyTransform` to apply a `PTransform[_ >: PCollection[T], PCollection[U]]` and get a `SCollection[U]`
- `SCollection#saveAsCustomOutput` to apply a `PTransform[_ >: PCollection[T], PDone]` (sink) and get a `ClosedTap[T]`.


## Aggregations

## Double functions

## Approximations

countApproxDistinct, quantilesApprox

## Materialization

com.spotify.scio.values.SCollection.materialize

## Batching

Elements can be batched

[`batch`](com.spotify.scio.values.SCollection.batch)
[`batchByteSized`](com.spotify.scio.values.SCollection.batchByteSized)
[`batchWeighted`](com.spotify.scio.values.SCollection.batchWeighted)
[`batchByKey`](com.spotify.scio.values.PairSCollectionFunctions.batchByKey)
[`batchByteSizedByKey`](com.spotify.scio.values.PairSCollectionFunctions.batchByteSizedByKey)
[`batchWeightedByKey`](com.spotify.scio.values.PairSCollectionFunctions.batchWeightedByKey)
