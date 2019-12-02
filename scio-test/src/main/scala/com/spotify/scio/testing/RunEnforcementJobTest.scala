/*
 * Copyright 2019 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.spotify.scio.testing

import com.spotify.scio.testing.{JobTest => InnerJobTest}
import org.scalactic.source.Position
import org.scalatest._

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import org.scalatest.flatspec.AnyFlatSpec

/**
 * Trait that enforces [[JobTest.Builder.run]] is called.
 */
trait RunEnforcementJobTest extends AnyFlatSpec { this: PipelineSpec =>

  private val tests = ArrayBuffer.empty[InnerJobTest.Builder]

  def JobTest[T: ClassTag]: InnerJobTest.Builder = {
    val jt = InnerJobTest[T]
    tests += jt
    jt
  }

  private[testing] def JobTest[T: ClassTag](enforceRun: Boolean = true): InnerJobTest.Builder = {
    val jt = InnerJobTest[T]
    if (enforceRun) tests += jt
    jt
  }

  def JobTest(className: String): InnerJobTest.Builder = {
    val jt = InnerJobTest(className)
    tests += jt
    jt
  }

  override protected def withFixture(test: NoArgTest): Outcome = {
    // Tests within Suites are executed sequentially, thus we need to clear the tests, if
    // ParallelTestExecution was enabled, clear is obsolete given the OneInstancePerTest
    tests.clear()
    val outcome = super.withFixture(test)
    if (outcome.isSucceeded) {
      val notRun = tests.filterNot(_.wasRunInvoked)
      if (notRun.nonEmpty) {
        val m = notRun.mkString(start = "Missing run(): ", sep = "\nMissing run(): ", end = "")
        Failed(s"Did you forget run()?\n$m")(test.pos.getOrElse(Position.here))
      } else {
        outcome
      }
    } else {
      outcome
    }
  }
}
