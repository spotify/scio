package fix.v0_8_0

import com.spotify.scio.values.SCollection

object FixJoinNames {

  def changeJoinNames(
    lhs: SCollection[(Int, String)],
    rhs: SCollection[(Int, String)]
  ): SCollection[(Int, (String, Option[String]))] = {
    lhs.hashLeftOuterJoin(rhs)
    lhs.sparseFullOuterJoin(rhs, 3)
    lhs.skewedLeftOuterJoin(rhs)
  }

  def changeNamesAndArgs(
    lhs: SCollection[(Int, String)],
    rightHS: SCollection[(Int, String)]
  ): SCollection[(Int, (String, Option[String]))] = {
    lhs.hashLeftOuterJoin(rhs = rightHS)
    lhs.sparseFullOuterJoin(rhs = rightHS, 3)
    lhs.sparseFullOuterJoin(rhs = rightHS, rhsNumKeys = 3)
    lhs.skewedLeftOuterJoin(rhs = rightHS)
  }

  def changeArgs(lhs: SCollection[(Int, String)], rightHS: SCollection[(Int, String)]): Unit = {
    lhs.join(rhs = rightHS)
    lhs.fullOuterJoin(rhs = rightHS)
    lhs.leftOuterJoin(rhs = rightHS)
    lhs.rightOuterJoin(rhs = rightHS)
    lhs.sparseFullOuterJoin(rhs = rightHS, rhsNumKeys = 4L, fpProb = 0.01d)
    lhs.sparseLeftOuterJoin(rhs = rightHS, rhsNumKeys = 4)
    lhs.sparseRightOuterJoin(rhs = rightHS, rhsNumKeys = 2)
    lhs.cogroup(rhs = rightHS)
    lhs.cogroup(rhs1 = rightHS, rhs2 = rightHS)
    lhs.cogroup(rhs1 = rightHS, rhs2 = rightHS, rhs3 = rightHS)
    lhs.groupWith(rhs = rightHS)
    lhs.groupWith(rhs1 = rightHS, rhs2 = rightHS)
    lhs.groupWith(rhs1 = rightHS, rhs2 = rightHS, rhs3 = rightHS)
    lhs.sparseLookup(rhs = rightHS, thisNumKeys = 1)
    lhs.sparseLookup(rhs1 = rightHS, rhs2 = rightHS, 3)

    lhs.skewedJoin(rhs = rightHS)
    lhs.skewedLeftOuterJoin(rhs = rightHS)
    lhs.skewedFullOuterJoin(rhs = rightHS)

    lhs.hashJoin(rhs = rightHS)
    lhs.hashFullOuterJoin(rhs = rightHS)
    lhs.hashLeftOuterJoin(rhs = rightHS)
    lhs.hashIntersectByKey(rhs = rightHS.map(a => a._1))
  }

  def example(lhs: SCollection[(Int, String)], right: SCollection[String]): Unit = {
    def hashLeftJoin(
      lhs: SCollection[(Int, String)]
    ): SCollection[(Int, (String, Option[String]))] =
      lhs.hashLeftOuterJoin(rhs = right.map(a => (a.length, a)))

    def sparseOuterJoin(a: String): Int =
      a.length

    def skewedLeftJoin(that: String): Int =
      that.length

    hashLeftJoin(lhs)
    sparseOuterJoin("test")
    skewedLeftJoin("test")
  }
}
