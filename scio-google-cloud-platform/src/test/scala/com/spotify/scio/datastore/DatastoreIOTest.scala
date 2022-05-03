/*
 * Copyright 2020 Spotify AB.
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

package com.spotify.scio.datastore

import com.spotify.scio.ContextAndArgs
import com.spotify.scio.testing._
import com.google.datastore.v1.Entity
import com.google.datastore.v1.client.DatastoreHelper

import java.util.Collections

object DatastoreJob {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    sc.datastore(args("input"), null, null)
      .saveAsDatastore(args("output"))
    sc.run()
    ()
  }
}

class DatastoreIOTest extends PipelineSpec with ScioIOSpec {

  "DatastoreIO" should "work" in {
    val xs = (1 to 100).map { x =>
      Entity
        .newBuilder()
        .putProperties("int", DatastoreHelper.makeValue(x).build())
        .build()
    }
    testJobTest(xs)(DatastoreIO(_))(_.datastore(_, null))(_.saveAsDatastore(_))
  }

  def newEntity(i: Int): Entity =
    Entity
      .newBuilder()
      .setKey(DatastoreHelper.makeKey())
      .putAllProperties(Collections.singletonMap("int_field", DatastoreHelper.makeValue(i).build()))
      .build()

  def testDatastore(xs: Seq[Entity]): Unit =
    JobTest[DatastoreJob.type]
      .args("--input=store.in", "--output=store.out")
      .input(DatastoreIO("store.in"), (1 to 3).map(newEntity))
      .output(DatastoreIO("store.out"))(coll => coll should containInAnyOrder(xs))
      .run()

  it should "pass correct DatastoreJob" in {
    testDatastore((1 to 3).map(newEntity))
  }

  it should "fail incorrect DatastoreJob" in {
    an[AssertionError] should be thrownBy {
      testDatastore((1 to 2).map(newEntity))
    }
    an[AssertionError] should be thrownBy {
      testDatastore((1 to 4).map(newEntity))
    }
  }

}
