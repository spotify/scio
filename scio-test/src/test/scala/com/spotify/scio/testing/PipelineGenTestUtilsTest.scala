package com.spotify.scio.testing

import org.scalacheck.Gen
import org.scalacheck.rng.Seed

class PipelineGenTestUtilsTest extends PipelineGenTestUtils with PipelineSpec {
  "PipelineTestUtils.withGen" should "generate a test case and have an optional return value" in {
    val x = withGen(Gen.const(1))(i => i)
    assert(x.contains(1))
  }

  it should "return None on test case generation failure, not throw" in {
    val x = withGen(Gen.const(1).suchThat(_ => false))(_ => ())
    assert(x.isEmpty)
  }

  it should "propagate errors" in {
    assertThrows[RuntimeException] {
      withGen(Gen.const(1)) { _ =>
        throw new RuntimeException("xxx")
      }
    }
  }

  it should "accept a static seed" in {
    val seed = Seed.random()
    val x = withGen(Gen.choose(1, 10), seed)(i => i)
    val y = withGen(Gen.choose(1, 10), seed)(i => i)
    assert(x == y)
  }

  it should "accept a base64 string seed" in {
    val seed = Seed.random().toBase64
    val x = withGen(Gen.choose(1, 10), seed)(i => i)
    val y = withGen(Gen.choose(1, 10), seed)(i => i)
    assert(x == y)
  }

}
