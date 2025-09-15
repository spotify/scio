package fix.v0_10_0

import com.spotify.scio.values.SCollection

object FixCoderPropagation {
  def topExample(in: SCollection[Int]): Unit =
    in.top(10)(Ordering[Int].reverse)

  def topByKeyExample(in: SCollection[(String, Int)]): Unit =
    in.topByKey(10)(Ordering[Int].reverse)

  def quantileApproxExample(in: SCollection[Int]): Unit =
    in.quantilesApprox(10)(Ordering[Int].reverse)

  def approxQuantilesByKeyExample(in: SCollection[(String, Int)]): Unit =
    in.approxQuantilesByKey(10)(Ordering[Int].reverse)
}
