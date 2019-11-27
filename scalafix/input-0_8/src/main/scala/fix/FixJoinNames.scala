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
    lhs.leftOuterJoin(rhs)
    lhs.hashLeftJoin(rhs)
    lhs.sparseOuterJoin(rhs, 3)
    lhs.skewedLeftJoin(rhs)
  }

  def changeNamesAndArgs(
    lhs: SCollection[(Int, String)],
    rhs: SCollection[(Int, String)]
  ): SCollection[(Int, (String, Option[String]))] = {
    lhs.leftOuterJoin(that = rhs)
    lhs.hashLeftJoin(that = rhs)
    lhs.sparseOuterJoin(that = rhs, 3)
    lhs.sparseOuterJoin(that = rhs, thatNumKeys = 3)
    lhs.skewedLeftJoin(that = rhs)
  }
}
