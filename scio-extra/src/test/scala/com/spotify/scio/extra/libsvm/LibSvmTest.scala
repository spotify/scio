package com.spotify.scio.extra.libsvm

import breeze.linalg.SparseVector
import com.spotify.scio.testing.PipelineSpec

class LibSvmTest extends PipelineSpec {
  val expected = List(
    (0.0, SparseVector[Double](34)((0,1), (8,1), (18,1), (20,1), (23,1), (33,1))),
    (1.0, SparseVector[Double](34)((2,1), (8,1), (18,1), (20,1), (29,1), (33,1))),
    (0.0, SparseVector[Double](34)((0,1), (8,1), (19,1), (20,1), (23,1), (33,1)))
  )

  val data = List(
    "0 1:1 9:1 19:1 21:1 24:1 34:1",
    "1 3:1 9:1 19:1 21:1 30:1 34:1",
    "0 1:1 9:1 20:1 21:1 24:1 34:1"
  )

  "LibSvmTest" should "parse libsvm files" in {
    runWithContext{ sc =>
      val res = sc.libSVMCollection(sc.parallelize(data))
      res should containInAnyOrder (expected)
    }
  }

  "LibSvmTest" should "parse libsvm files with length" in {
    runWithContext{ sc =>
      val res = sc.libSVMCollection(sc.parallelize(data), 34)
      res should containInAnyOrder (expected)
    }
  }
}
