/*
rule = ConsistenceJoinNames
 */
package fix

import com.spotify.scio.values.SCollection

object FixJoinNames {

  def changeJoinNames(lhs: SCollection[(Int, String)], rhs: SCollection[(Int, String)]
  ): SCollection[(Int, (String, Option[String]))] = {
    lhs.hashLeftJoin(rhs)
    lhs.sparseOuterJoin(rhs, 3)
    lhs.skewedLeftJoin(rhs)
  }

  def example(): Unit = {
    def hashLeftJoin(a: String): Int =  {
      a.length
    }

    def sparseOuterJoin(a: String): Int = {
      a.length
    }

    def skewedLeftJoin(that: String): Int = {
      that.length
    }

    hashLeftJoin("test")
    sparseOuterJoin("test")
    skewedLeftJoin("test")
  }
}
