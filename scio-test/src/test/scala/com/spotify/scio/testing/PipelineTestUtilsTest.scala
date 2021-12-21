package com.spotify.scio.testing

import org.scalacheck.Gen
import org.scalacheck.rng.Seed

class PipelineTestUtilsTest extends PipelineSpec {
  "PipelineTestUtils" should "generate a test case and have an optional return value" in {
    val xxx = withGen(Gen.const(1))(i => i)
    assert(xxx.contains(1))
  }

  it should "return None on test case generation failure, not throw" in {
    val xxx = withGen(Gen.const(1).suchThat(_ => false))(_ => ())
    assert(xxx.isEmpty)
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
    val xxx = withGen(Gen.choose(1, 10), seed)(i => i)
    val yyy = withGen(Gen.choose(1, 10), seed)(i => i)
    assert(xxx == yyy)
  }

  it should "accept a base64 string seed" in {
    val seed = Seed.random().toBase64
    val xxx = withGen(Gen.choose(1, 10), seed)(i => i)
    val yyy = withGen(Gen.choose(1, 10), seed)(i => i)
    assert(xxx == yyy)
  }

}
