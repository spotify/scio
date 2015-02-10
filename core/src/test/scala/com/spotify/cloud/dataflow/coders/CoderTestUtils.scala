package com.spotify.cloud.dataflow.coders

import com.google.cloud.dataflow.sdk.coders.Coder
import com.google.cloud.dataflow.sdk.util.CoderUtils

object CoderTestUtils {

  def testRoundTrip[T](coder: Coder[T], value: T): Boolean = {
    val bytes = CoderUtils.encodeToByteArray(coder, value)
    val result = CoderUtils.decodeFromByteArray(coder, bytes)
    result == value
  }

}
