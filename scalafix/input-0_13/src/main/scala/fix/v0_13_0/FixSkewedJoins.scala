/*
rule = FixSkewedJoins
 */
package fix.v0_13_0

import com.spotify.scio.values.SCollection

object FixSkewedJoins {

  // pair SCollection
  val scoll: SCollection[(String, Int)] = ???
  val rhs: SCollection[(String, String)] = ???

  // unamed parameters
  scoll.skewedJoin(rhs, 9000, 0.001, 42, 1e-10, 1.0, true)
  scoll.skewedLeftOuterJoin(rhs, 9000, 0.001, 42, 1e-10, 1.0, true)
  scoll.skewedFullOuterJoin(rhs, 9000, 0.001, 42, 1e-10, 1.0, true)

  // named parameters
  scoll.skewedJoin(
    rhs = rhs,
    hotKeyThreshold = 9000,
    eps = 0.001,
    seed = 42,
    delta = 1e-10,
    sampleFraction = 1.0,
    withReplacement = true
  )
  scoll.skewedLeftOuterJoin(
    rhs = rhs,
    hotKeyThreshold = 9000,
    eps = 0.001,
    seed = 42,
    delta = 1e-10,
    sampleFraction = 1.0,
    withReplacement = true
  )
  scoll.skewedFullOuterJoin(
    rhs = rhs,
    hotKeyThreshold = 9000,
    eps = 0.001,
    seed = 42,
    delta = 1e-10,
    sampleFraction = 1.0,
    withReplacement = true
  )
}