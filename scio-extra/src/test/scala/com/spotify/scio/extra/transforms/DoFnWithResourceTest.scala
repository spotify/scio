/*
 * Copyright 2017 Spotify AB.
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

package com.spotify.scio.extra.transforms

import java.util.UUID

import com.spotify.scio.extra.transforms.DoFnWithResource.ResourceType
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms.DoFnTester
import org.apache.beam.sdk.util.SerializableUtils
import org.scalatest._

import scala.collection.JavaConverters._

class DoFnWithResourceTest extends FlatSpec with Matchers {

  private def cloneAndProcess(doFn: DoFnWithResource[String, String, TestResource]) = {
    val clone = SerializableUtils.ensureSerializable(doFn)
    clone.setup()
    clone
  }

  "DoFnWithResource" should "support per class resources" in {
    // instances on local main
    val i1 = new DoFnWithPerClassResource
    val i2 = new DoFnWithPerClassResource

    // copies on remote worker cores
    val c1 = cloneAndProcess(i1)
    val c2 = cloneAndProcess(i1)
    val c3 = cloneAndProcess(i2)
    val c4 = cloneAndProcess(i2)

    c1.getResource should equal (c2.getResource)
    c1.getResource should equal (c3.getResource)
    c1.getResource should equal (c4.getResource)

    DoFnTester.of(c1).processBundle("a", "b", "c").asScala should equal (Seq("A", "B", "C"))
  }

  it should "support per instance resources" in {
    // instances on local main
    val i1 = new DoFnWithPerInstanceResource
    val i2 = new DoFnWithPerInstanceResource

    // copies on remote worker cores
    val c1 = cloneAndProcess(i1)
    val c2 = cloneAndProcess(i1)
    val c3 = cloneAndProcess(i2)
    val c4 = cloneAndProcess(i2)

    c1.getResource should equal (c2.getResource)
    c1.getResource shouldNot equal (c3.getResource)
    c1.getResource shouldNot equal (c4.getResource)

    c3.getResource should equal (c4.getResource)
    c3.getResource shouldNot equal (c1.getResource)
    c3.getResource shouldNot equal (c2.getResource)

    DoFnTester.of(c1).processBundle("a", "b", "c").asScala should equal (Seq("A", "B", "C"))
  }

  it should "support per core resources" in {
    // instances on local main
    val i1 = new DoFnWithPerCoreResource
    val i2 = new DoFnWithPerCoreResource

    // copies on remote worker cores
    val c1 = cloneAndProcess(i1)
    val c2 = cloneAndProcess(i1)
    val c3 = cloneAndProcess(i2)
    val c4 = cloneAndProcess(i2)

    c1.getResource shouldNot equal (c2.getResource)
    c1.getResource shouldNot equal (c3.getResource)
    c1.getResource shouldNot equal (c4.getResource)

    c2.getResource shouldNot equal (c1.getResource)
    c2.getResource shouldNot equal (c3.getResource)
    c2.getResource shouldNot equal (c4.getResource)

    c3.getResource shouldNot equal (c1.getResource)
    c3.getResource shouldNot equal (c2.getResource)
    c3.getResource shouldNot equal (c4.getResource)

    DoFnTester.of(c1).processBundle("a", "b", "c").asScala should equal (Seq("A", "B", "C"))
  }

}

private case class TestResource(id: String) {
  def processElement(input: String): String = input.toUpperCase
}

abstract private class BaseDoFn extends DoFnWithResource[String, String, TestResource] {
  override def createResource(): TestResource = TestResource(UUID.randomUUID().toString)
  @ProcessElement
  def processElement(c: ProcessContext): Unit = c.output(getResource.processElement(c.element()))
}

private class DoFnWithPerClassResource extends BaseDoFn {
  override def getResourceType: DoFnWithResource.ResourceType = ResourceType.PER_CLASS
}

private class DoFnWithPerInstanceResource extends BaseDoFn {
  override def getResourceType: DoFnWithResource.ResourceType = ResourceType.PER_INSTANCE
}

private class DoFnWithPerCoreResource extends BaseDoFn {
  override def getResourceType: DoFnWithResource.ResourceType = ResourceType.PER_CLONE
}
