/*
Copyright 2013 Twitter, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */

package com.spotify.scio.vendor.chill

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

object RegistrationIdsSpec {
  private def registeredEntries(k: KryoBase) =
    Stream
      .from(0)
      .map(k.getRegistration)
      .takeWhile(_ != null)
      .map(r => s"${r.getId} -> ${r.getType}")

  private class ErrorClue(k: KryoBase, scope: String) {
    override def toString: String =
      s"""
         |
         |This test ($getClass)
         |will fail for $scope, most probably because the order of
         |registration IDs has changed or a registration was added or
         |removed. If that was intended, here is the list of entries
         |that are currently found, so you can update the test's
         |expected values:
         |
         |${registeredEntries(k).mkString("\n")}
         |
         |""".stripMargin
  }
}

class RegistrationIdsSpec extends AnyWordSpec with Matchers {
  import RegistrationIdsSpec._
  import RegistrationIdsSpecData._

  """
    |Projects using chill to long term persist serialized data (for example in event
    |sourcing scenarios) depend on the registration IDs of the pre-registered
    |classes being stable. Therefore, it is important that updates to chill avoid
    |changing registration IDs of the pre-registered classes as far as possible.
    |When changing registration IDs becomes necessary, details of the changes should
    |be mentioned in the release notes.
    |
    |For the ScalaKryoInstantiators, the registered classes""".stripMargin
    .should {
      val compatibility = classOf[AllScalaRegistrar_0_9_5].getSimpleName
      (s"be as expected for the backward compatibility layer $compatibility,\n" +
        "  i.e. contain the list of registrations defined in this test.").in {
        val k = new KryoBase
        new AllScalaRegistrar_0_9_5().apply(k)
        val clue = new ErrorClue(k, compatibility)
        registeredEntries(k).zip(Entries_0_9_5).foreach { case (r, c) =>
          assert(r == c, clue)
        }
      }

      val current = classOf[AllScalaRegistrar].getSimpleName
      (s"be as expected for the current $current,\n" +
        "  i.e. contain the list of registrations defined in this test.").in {
        val k = new KryoBase
        new AllScalaRegistrar().apply(k)
        val clue = new ErrorClue(k, compatibility)
        registeredEntries(k).zip(CurrentEntries).foreach { case (r, c) =>
          assert(r == c, clue)
        }
      }
    }
}
