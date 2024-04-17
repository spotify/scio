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

package com.spotify.scio.transforms

import com.spotify.scio.testing._
import com.spotify.scio.transforms.DoFnWithResource.ResourceType
import org.apache.beam.sdk.transforms.DoFn.{Element, OutputReceiver, ProcessElement}
import org.apache.beam.sdk.util.SerializableUtils
import org.scalatest.BeforeAndAfter

class DoFnWithResourceTest extends PipelineSpec with BeforeAndAfter {

  before {
    ClosableResourceCounters.resetCounters()
  }

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

    c1.getResource shouldBe c2.getResource
    c1.getResource shouldBe c3.getResource
    c1.getResource shouldBe c4.getResource

    runWithData(Seq("a", "b", "c"))(_.parDo(c1)) should contain theSameElementsAs Seq("A", "B", "C")
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

    c1.getResource shouldBe c2.getResource
    c1.getResource should not be c3.getResource
    c1.getResource should not be c4.getResource

    c3.getResource shouldBe c4.getResource
    c3.getResource should not be c1.getResource
    c3.getResource should not be c2.getResource

    runWithData(Seq("a", "b", "c"))(_.parDo(c1)) should contain theSameElementsAs Seq("A", "B", "C")
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

    c1.getResource should not be c2.getResource
    c1.getResource should not be c3.getResource
    c1.getResource should not be c4.getResource

    c2.getResource should not be c1.getResource
    c2.getResource should not be c3.getResource
    c2.getResource should not be c4.getResource

    c3.getResource should not be c1.getResource
    c3.getResource should not be c2.getResource
    c3.getResource should not be c4.getResource

    runWithData(Seq("a", "b", "c"))(_.parDo(c1)) should contain theSameElementsAs Seq("A", "B", "C")
  }

  it should "support per class closeable resources" in {
    val i1 = new DoFnWithPerClassResourceCloseable
    val i2 = new DoFnWithPerClassResourceCloseable

    runWithData(Seq("a", "b", "c"))(_.parDo(i1).parDo(i2)) should contain theSameElementsAs Seq(
      "A",
      "B",
      "C"
    )

    ClosableResourceCounters.clientsOpened.get() should equal(1)
    ClosableResourceCounters.allResourcesClosed should be(true)
  }

  it should "support per instance closeable resources" in {
    val i1 = new DoFnWithPerInstanceResourceCloseable
    val i2 = new DoFnWithPerInstanceResourceCloseable

    runWithData(Seq("a", "b", "c"))(_.parDo(i1).parDo(i2)) should contain theSameElementsAs Seq(
      "A",
      "B",
      "C"
    )

    ClosableResourceCounters.clientsOpened.get() should equal(2)
    ClosableResourceCounters.allResourcesClosed should be(true)
  }

  it should "support per core closeable resources" in {
    val i1 = new DoFnWithPerCoreResourceCloseable
    val i2 = new DoFnWithPerCoreResourceCloseable

    runWithData(Seq("a", "b", "c"))(_.parDo(i1).parDo(i2)) should contain theSameElementsAs Seq(
      "A",
      "B",
      "C"
    )

    ClosableResourceCounters.clientsOpened.get() should be >= 2
    ClosableResourceCounters.allResourcesClosed should be(true)
  }
}

private class TestResource {
  def processElement(input: String): String = input.toUpperCase
}

private class TestCloseableResource extends CloseableResource {
  def processElement(input: String): String = {
    ensureOpen()
    input.toUpperCase
  }
}

abstract private class BaseDoFn extends DoFnWithResource[String, String, TestResource] {
  override def createResource(): TestResource = new TestResource
  @ProcessElement
  def processElement(@Element element: String, out: OutputReceiver[String]): Unit =
    out.output(getResource.processElement(element))
}

private class DoFnWithPerClassResource extends BaseDoFn {
  override def getResourceType: DoFnWithResource.ResourceType =
    ResourceType.PER_CLASS
}

private class DoFnWithPerInstanceResource extends BaseDoFn {
  override def getResourceType: DoFnWithResource.ResourceType =
    ResourceType.PER_INSTANCE
}

private class DoFnWithPerCoreResource extends BaseDoFn {
  override def getResourceType: DoFnWithResource.ResourceType =
    ResourceType.PER_CLONE
}

abstract private class BaseDoFnCloseable
    extends DoFnWithResource[String, String, TestCloseableResource] {
  override def createResource(): TestCloseableResource =
    new TestCloseableResource()
  @ProcessElement
  def processElement(@Element element: String, out: OutputReceiver[String]): Unit =
    out.output(getResource.processElement(element))
}

private class DoFnWithPerClassResourceCloseable extends BaseDoFnCloseable {
  override def getResourceType: DoFnWithResource.ResourceType =
    ResourceType.PER_CLASS
}

private class DoFnWithPerInstanceResourceCloseable extends BaseDoFnCloseable {
  override def getResourceType: DoFnWithResource.ResourceType =
    ResourceType.PER_INSTANCE
}

private class DoFnWithPerCoreResourceCloseable extends BaseDoFnCloseable {
  override def getResourceType: DoFnWithResource.ResourceType =
    ResourceType.PER_CLONE
}
