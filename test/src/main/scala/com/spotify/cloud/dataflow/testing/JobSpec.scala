package com.spotify.cloud.dataflow.testing

import org.scalatest.{FlatSpec, Matchers}

/**
 * Trait for unit testing of Dataflow jobs.
 * To be used with [[com.spotify.cloud.dataflow.testing.JobTest JobTest]].
 */
trait JobSpec extends FlatSpec with Matchers with PCollectionMatcher
