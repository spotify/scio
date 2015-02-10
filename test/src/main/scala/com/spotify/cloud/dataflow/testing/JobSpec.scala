package com.spotify.cloud.dataflow.testing

import org.scalatest.{FlatSpec, Matchers}

trait JobSpec extends FlatSpec with Matchers with PCollectionMatchers
