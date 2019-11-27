/*
rule = ConsistenceJoinNames
 */
package fix

import com.spotify.scio.values.SCollection

object FixJoinNames {

  def example(lhs: SCollection[(Int, String)], rhs: SCollection[(Int, String)]
  ): SCollection[(Int, (String, Option[String]))] = {
    lhs.leftOuterJoin(that = rhs)
    lhs.hashLeftJoin(rhs)
    lhs.sparseOuterJoin(rhs, 3)
    lhs.skewedLeftJoin(rhs)
  }

}
