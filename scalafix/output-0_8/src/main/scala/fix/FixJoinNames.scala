package fix

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

  def changeNamesAndArgs(lhs: SCollection[(Int, String)], rightHS: SCollection[(Int, String)]
  ): SCollection[(Int, (String, Option[String]))] = {
    lhs.leftOuterJoin(that = rightHS)
    lhs.hashLeftOuterJoin(rhs = rightHS)
    lhs.sparseFullOuterJoin(rhs = rightHS, 3)
    lhs.sparseFullOuterJoin(rhs = rightHS, rhsNumKeys = 3)
    lhs.skewedLeftOuterJoin(rhs = rightHS)
  }

  def example(): Unit = {
    def hashLeftJoin(a: String): Int =
      a.length

    def sparseOuterJoin(a: String): Int =
      a.length

    def skewedLeftJoin(that: String): Int =
      that.length

    hashLeftJoin("test")
    sparseOuterJoin("test")
    skewedLeftJoin("test")
  }
}
