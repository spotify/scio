package fix.v0_13_0

import com.spotify.scio.values.SCollection
import com.spotify.scio.values.HotKeyMethod

object FixSkewedJoins {

  // pair SCollection
  val scoll: SCollection[(String, Int)] = ???
  val rhs: SCollection[(String, String)] = ???

  // unamed parameters
  scoll.skewedJoin(rhs = rhs, hotKeyMethod = HotKeyMethod.Threshold(9000), cmsEps = 0.001d, cmsDelta = 1E-10d, cmsSeed = 42, sampleFraction = 1.0d, sampleWithReplacement = true)
  scoll.skewedLeftOuterJoin(rhs = rhs, hotKeyMethod = HotKeyMethod.Threshold(9000), cmsEps = 0.001d, cmsDelta = 1E-10d, cmsSeed = 42, sampleFraction = 1.0d, sampleWithReplacement = true)
  scoll.skewedFullOuterJoin(rhs = rhs, hotKeyMethod = HotKeyMethod.Threshold(9000), cmsEps = 0.001d, cmsDelta = 1E-10d, cmsSeed = 42, sampleFraction = 1.0d, sampleWithReplacement = true)

  // named parameters
  scoll.skewedJoin(rhs = rhs, hotKeyMethod = HotKeyMethod.Threshold(9000), cmsEps = 0.001d, cmsDelta = 1E-10d, cmsSeed = 42, sampleFraction = 1.0d, sampleWithReplacement = true)
  scoll.skewedLeftOuterJoin(rhs = rhs, hotKeyMethod = HotKeyMethod.Threshold(9000), cmsEps = 0.001d, cmsDelta = 1E-10d, cmsSeed = 42, sampleFraction = 1.0d, sampleWithReplacement = true)
  scoll.skewedFullOuterJoin(rhs = rhs, hotKeyMethod = HotKeyMethod.Threshold(9000), cmsEps = 0.001d, cmsDelta = 1E-10d, cmsSeed = 42, sampleFraction = 1.0d, sampleWithReplacement = true)
}