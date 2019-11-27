package fix

import com.spotify.scio.values.SCollection

object FixJoinNames {

  def changeJoinNames(
    lhs: SCollection[(Int, String)],
    rhs: SCollection[(Int, String)]
  ): SCollection[(Int, (String, Option[String]))] = {
    lhs.leftOuterJoin(rhs)
    lhs.hashLeftOuterJoin(rhs)
    lhs.sparseFullOuterJoin(rhs, 3)
    lhs.skewedLeftOuterJoin(rhs)
  }

  def changeNamesAndArgs(
    lhs: SCollection[(Int, String)],
    rhs: SCollection[(Int, String)]
  ): SCollection[(Int, (String, Option[String]))] = {
    lhs.leftOuterJoin(that = rhs)
    lhs.hashLeftOuterJoin(right = rhs)
    lhs.sparseFullOuterJoin(right = rhs, 3)
    lhs.sparseFullOuterJoin(right = rhs, thatNumKeys = 3)
    lhs.skewedLeftOuterJoin(right = rhs)
  }
}
