/*
rule = ConsistenceJoinNames
 */
package fix

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

  def changeNamesAndArgs(lhs: SCollection[(Int, String)], rightHS: SCollection[(Int, String)]
  ): SCollection[(Int, (String, Option[String]))] = {
    lhs.leftOuterJoin(that = rightHS)
    lhs.hashLeftJoin(that = rightHS)
    lhs.sparseOuterJoin(that = rightHS, 3)
    lhs.sparseOuterJoin(that = rightHS, thatNumKeys = 3)
    lhs.skewedLeftJoin(that = rightHS)
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
