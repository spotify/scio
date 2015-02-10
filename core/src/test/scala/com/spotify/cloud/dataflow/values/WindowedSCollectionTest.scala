package com.spotify.cloud.dataflow.values

import com.spotify.cloud.dataflow.testing.PipelineTest
import org.joda.time.{Duration, Instant}

class WindowedSCollectionTest extends PipelineTest {

  "WindowedSCollection" should "support filter()"  in {
    runWithContext { context =>
      val p = context.parallelizeTimestamped(Seq("a", "b", "c", "d"), Seq(1, 2, 3, 4).map(new Instant(_)))
      val r = p.toWindowed
        .filter(v => v.timestamp.getMillis % 2 == 0).toSCollection.withTimestamp().map(kv => (kv._1, kv._2.getMillis))
      r.internal should containInAnyOrder (("b", 2L), ("d", 4L))
    }
  }

  it should "support flatMap()"  in {
    runWithContext { context =>
      val p = context.parallelizeTimestamped(Seq("a", "b"), Seq(1, 2).map(new Instant(_)))
      val r = p.toWindowed
        .flatMap(v => Seq(v.copy(v.value + "1"), v.copy(v.value + "2")))
        .toSCollection.withTimestamp().map(kv => (kv._1, kv._2.getMillis))
      r.internal should containInAnyOrder (("a1", 1L), ("a2", 1L), ("b1", 2L), ("b2", 2L))
    }
  }

  it should "support keyBy()"  in {
    runWithContext { context =>
      val p = context.parallelizeTimestamped(Seq("a", "b"), Seq(1, 2).map(new Instant(_)))
      val r = p.toWindowed.keyBy(v => v.value + v.timestamp.getMillis).toSCollection
      r.internal should containInAnyOrder (("a1", "a"), ("b2", "b"))
    }
  }

  it should "support map()"  in {
    runWithContext { context =>
      val p = context.parallelizeTimestamped(Seq("a", "b"), Seq(1, 2).map(new Instant(_)))
      val r = p.toWindowed.map(v => v.copy(v.value + v.timestamp.getMillis)).toSCollection
      r.internal should containInAnyOrder ("a1", "b2")
    }
  }

}
