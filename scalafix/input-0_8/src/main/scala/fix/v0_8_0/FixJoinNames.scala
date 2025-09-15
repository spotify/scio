/*
rule = ConsistenceJoinNames
 */
package fix.v0_8_0

import com.spotify.scio.values.SCollection

object FixJoinNames {

  def changeJoinNames(
    lhs: SCollection[(Int, String)],
    rhs: SCollection[(Int, String)]
  ): SCollection[(Int, (String, Option[String]))] = {
    lhs.hashLeftJoin(rhs)
    lhs.sparseOuterJoin(rhs, 3)
    lhs.skewedLeftJoin(rhs)
  }

  def changeNamesAndArgs(
    lhs: SCollection[(Int, String)],
    rightHS: SCollection[(Int, String)]
  ): SCollection[(Int, (String, Option[String]))] = {
    lhs.hashLeftJoin(that = rightHS)
    lhs.sparseOuterJoin(that = rightHS, 3)
    lhs.sparseOuterJoin(that = rightHS, thatNumKeys = 3)
    lhs.skewedLeftJoin(that = rightHS)
  }

  def changeArgs(lhs: SCollection[(Int, String)], rightHS: SCollection[(Int, String)]): Unit = {
    lhs.join(that = rightHS)
    lhs.fullOuterJoin(that = rightHS)
    lhs.leftOuterJoin(that = rightHS)
    lhs.rightOuterJoin(that = rightHS)
    lhs.sparseOuterJoin(that = rightHS, thatNumKeys = 4L, fpProb = 0.01)
    lhs.sparseLeftOuterJoin(that = rightHS, thatNumKeys = 4)
    lhs.sparseRightOuterJoin(that = rightHS, thatNumKeys = 2)
    lhs.cogroup(that = rightHS)
    lhs.cogroup(that1 = rightHS, that2 = rightHS)
    lhs.cogroup(that1 = rightHS, that2 = rightHS, that3 = rightHS)
    lhs.groupWith(that = rightHS)
    lhs.groupWith(that1 = rightHS, that2 = rightHS)
    lhs.groupWith(that1 = rightHS, that2 = rightHS, that3 = rightHS)
    lhs.sparseLookup(that = rightHS, thisNumKeys = 1)
    lhs.sparseLookup(that1 = rightHS, that2 = rightHS, 3)

    lhs.skewedJoin(that = rightHS)
    lhs.skewedLeftJoin(that = rightHS)
    lhs.skewedFullOuterJoin(that = rightHS)

    lhs.hashJoin(that = rightHS)
    lhs.hashFullOuterJoin(that = rightHS)
    lhs.hashLeftJoin(that = rightHS)
    lhs.hashIntersectByKey(that = rightHS.map(a => a._1))
  }

  def example(lhs: SCollection[(Int, String)], right: SCollection[String]): Unit = {
    def hashLeftJoin(
      lhs: SCollection[(Int, String)]
    ): SCollection[(Int, (String, Option[String]))] =
      lhs.hashLeftJoin(that = right.map(a => (a.length, a)))

    def sparseOuterJoin(a: String): Int =
      a.length

    def skewedLeftJoin(that: String): Int =
      that.length

    hashLeftJoin(lhs)
    sparseOuterJoin("test")
    skewedLeftJoin("test")
  }
}
