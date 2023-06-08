# Rollup

TODO

@scaladoc[](com.spotify.scio.extra.rollup.syntax.SCollectionSyntax.RollupOps)

case class UserStableMetadata(country: String, state: String)
case class UserChangingMetadata(playSource: Option[String], platform: Option[String])
case class Aggregates(skips: Int, plays: Int)
where input is something like:
(UserId, UserStableMetadata, UserChangingMetadata, Aggregates)

and the "rollup function" splats out every permutation of Some/None for UserChangingMetadata

input:
("u1", ("usa", "ca"), (Some("app"), Some("iphone")), (1, 2))
("u1", ("usa", "ca"), (Some("app"), Some("osx")), (2, 4))
("u2", ("usa", "ca"), (Some("app"), Some("android")), (3, 6))
("u2", ("usa", "ca"), (Some("app"), Some("iphone")), (5, 10))

// .map { case (_, dims, rollupDims, measure) => ((dims, rollupDims), (measure, 1L))}
(("usa", "ca"), (Some("app"), Some("iphone"))) -> ((1, 2), 1)
(("usa", "ca"), (Some("app"), Some("osx"))) -> ((2, 4), 1)
(("usa", "ca"), (Some("app"), Some("android"))) -> ((3, 6), 1)
(("usa", "ca"), (Some("app"), Some("iphone"))) -> ((5, 10), 1)

// .sumByKey
(("usa", "ca"), (Some("app"), Some("iphone"))) -> ((6, 12), 2)
(("usa", "ca"), (Some("app"), Some("osx"))) -> ((2, 4), 1)
(("usa", "ca"), (Some("app"), Some("android"))) -> ((3, 6), 1)

// flatMap { case (dims@(_, rollupDims), measure) =>
//    rollupFunction(rollupDims) .map((x: R) => dims.copy(_2 = x) -> measure)
// }
(("usa", "ca"), (Some("app"), Some("iphone"))) -> ((6, 12), 2)
(("usa", "ca"), (Some("app"), None)) -> ((6, 12), 2)
(("usa", "ca"), (None, Some("iphone"))) -> ((6, 12), 2)
(("usa", "ca"), (None, None)) -> ((6, 12), 2)
--
(("usa", "ca"), (Some("app"), Some("osx"))) -> ((2, 4), 1)
(("usa", "ca"), (Some("app"), None)) -> ((2, 4), 1)
(("usa", "ca"), (None, Some("osx"))) -> ((2, 4), 1)
(("usa", "ca"), (None, None)) -> ((2, 4), 1)
--
(("usa", "ca"), (Some("app"), Some("android"))) -> ((3, 6), 1)
(("usa", "ca"), (Some("app"), None)) -> ((3, 6), 1)
(("usa", "ca"), (None, Some("android"))) -> ((3, 6), 1)
(("usa", "ca"), (None, None)) -> ((3, 6), 1)

// note: starting over w/ input
// input.map { case (uniqueKey, dims, rollupDims, _) =>
//    ((uniqueKey, dims), rollupDims)
// }.groupByKey
("u1", ("usa", "ca")) 
    -> List(
        (Some("app"), Some("iphone")),
        (Some("app"), Some("osx"))
    )
("u2", ("usa", "ca"))
    -> List (
        (Some("app"), Some("android")),
        (Some("app"), Some("iphone")),
    )

// .filterValues(_.size > 1)
<same>

// contents of rollupMap before filtering:
(Some("app"), Some("osx")) -> 0L
(Some("app"), Some("iphone")) -> 0L
(None, Some("osx")) -> 0L
(None, Some("iphone")) -> 0L
(Some("app"), None) -> -1L
(None, None) -> -1L

// .flatMapValues { values =>
//   val rollupMap = collection.mutable.Map.empty[R, Long]
//   for (r <- values) {
//     for (newDim <- rollupFunction(r)) {
//       rollupMap(newDim) = rollupMap.getOrElse(newDim, 1L) - 1L
//     }
//   }
//   rollupMap.iterator.filter(_._2 < 0L)
//  }
("u1", ("usa", "ca")) -> ((Some("app"), None), -1L)
("u1", ("usa", "ca")) -> ((None, None), -1L)
("u2", ("usa", "ca")) -> ((Some("app"), None), -1L)
("u2", ("usa", "ca")) -> ((None, None), -1L)

// .map { case ((_, dims), (rollupDims, count)) => ((dims, rollupDims), (g.zero, count)) }
(("usa", "ca"), (Some("app"), None)) -> ((0, 0), -1L)
(("usa", "ca"), (None, None)) -> ((0, 0), -1L)
(("usa", "ca"), (Some("app"), None)) -> ((0, 0), -1L)
(("usa", "ca"), (None, None)) -> ((0, 0), -1L)

// .unionAll(List(doubleCounting, correctingCounts))
(("usa", "ca"), (Some("app"), Some("iphone"))) -> ((6, 12), 2)
(("usa", "ca"), (Some("app"), None)) -> ((6, 12), 2)
(("usa", "ca"), (None, Some("iphone"))) -> ((6, 12), 2)
(("usa", "ca"), (None, None)) -> ((6, 12), 2)
--
(("usa", "ca"), (Some("app"), Some("osx"))) -> ((2, 4), 1)
(("usa", "ca"), (Some("app"), None)) -> ((2, 4), 1)
(("usa", "ca"), (None, Some("osx"))) -> ((2, 4), 1)
(("usa", "ca"), (None, None)) -> ((2, 4), 1)
--
(("usa", "ca"), (Some("app"), Some("android"))) -> ((3, 6), 1)
(("usa", "ca"), (Some("app"), None)) -> ((3, 6), 1)
(("usa", "ca"), (None, Some("android"))) -> ((3, 6), 1)
(("usa", "ca"), (None, None)) -> ((3, 6), 1)
--
(("usa", "ca"), (Some("app"), None)) -> ((0, 0), -1L)
(("usa", "ca"), (None, None)) -> ((0, 0), -1L)
(("usa", "ca"), (Some("app"), None)) -> ((0, 0), -1L)
(("usa", "ca"), (None, None)) -> ((0, 0), -1L)

// .sumByKey, interim:
(("usa", "ca"), (Some("app"), Some("iphone"))) -> ((6, 12), 2)
(("usa", "ca"), (Some("app"), Some("osx"))) -> ((2, 4), 1)
(("usa", "ca"), (Some("app"), Some("android"))) -> ((3, 6), 1)
(("usa", "ca"), (None, Some("osx"))) -> ((2, 4), 1)
(("usa", "ca"), (None, Some("iphone"))) -> ((6, 12), 2)
(("usa", "ca"), (None, Some("android"))) -> ((3, 6), 1)
--
(("usa", "ca"), (Some("app"), None)) -> ((6, 12), 2)
(("usa", "ca"), (Some("app"), None)) -> ((2, 4), 1)
(("usa", "ca"), (Some("app"), None)) -> ((3, 6), 1)
(("usa", "ca"), (Some("app"), None)) -> ((0, 0), -1L)
(("usa", "ca"), (Some("app"), None)) -> ((0, 0), -1L)
--
(("usa", "ca"), (None, None)) -> ((6, 12), 2)
(("usa", "ca"), (None, None)) -> ((2, 4), 1) <-- count here
(("usa", "ca"), (None, None)) -> ((3, 6), 1) <-- and here
(("usa", "ca"), (None, None)) -> ((0, 0), -1L) <-- cancelled out by
(("usa", "ca"), (None, None)) -> ((0, 0), -1L) <-- and this one

// .sumByKey
(("usa", "ca"), (Some("app"), Some("iphone"))) -> ((6, 12), 2)
(("usa", "ca"), (Some("app"), Some("osx"))) -> ((2, 4), 1)
(("usa", "ca"), (Some("app"), Some("android"))) -> ((3, 6), 1)
(("usa", "ca"), (None, Some("osx"))) -> ((2, 4), 1)
(("usa", "ca"), (None, Some("iphone"))) -> ((6, 12), 2)
(("usa", "ca"), (None, Some("android"))) -> ((3, 6), 1)
--
(("usa", "ca"), (Some("app"), None)) -> ((11, 22), 2)
(("usa", "ca"), (None, None)) -> ((11, 22), 2)



```scala
import com.spotify.scio.values.SCollection



case class A(a: Option[String], b: Option[String], c: Option[String]) {
  def to: Set[A] = {
    val out = for {
      foo <- List(this.copy(a = None), this)
      bar <- List(foo.copy(b = None), foo)
      baz <- List(bar.copy(c = None), bar)
    } yield baz
    out.toSet
  }
}

case class Measure(numSkips: Int, numStreams: Int)

val elements: SCollection[(String, )] = ???



case class UserDimensions(
                           registrationFunnelV2: RegistrationFunnelV2,
                           registrationCountry: Country,
                           reportingProduct: ReportingProduct,
                           selfReportedAgeBucket: SelfReportedAgeBucket,
                           otherProductPeriodIds: List[String]
                         )

case class StreamMetricDimensions(
                                   platformType: Option[PlatformType],
                                   platformOs: Option[PlatformOs],
                                   contentType: Option[ContentType],
                                   appExperience: Option[AppExperience],
                                   isProgrammed: Option[Boolean]
                                 )
case class EcaMeasure(streamCount: Long, thirtySecondStreamCount: Long, msPlayed: Long)

(userId, (date, userDimensions), streamMetricDims, ecaMeasure)


R => Set[R] 

```