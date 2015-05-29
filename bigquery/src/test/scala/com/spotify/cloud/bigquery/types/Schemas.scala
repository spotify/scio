package com.spotify.cloud.bigquery.types

import org.joda.time.Instant

object Schemas {

  // primitives
  case class P1(f1: Int, f2: Long, f3: Float, f4: Double, f5: Boolean, f6: String, f7: Instant)
  case class P2(f1: Option[Int], f2: Option[Long], f3: Option[Float], f4: Option[Double],
                f5: Option[Boolean], f6: Option[String], f7: Option[Instant])
  case class P3(f1: List[Int], f2: List[Long], f3: List[Float], f4: List[Double],
                f5: List[Boolean], f6: List[String], f7: List[Instant])

  // records
  case class R1(f1: P1, f2: P2, f3: P3)
  case class R2(f1: Option[P1], f2: Option[P2], f3: Option[P3])
  case class R3(f1: List[P1], f2: List[P2], f3: List[P3])

}
