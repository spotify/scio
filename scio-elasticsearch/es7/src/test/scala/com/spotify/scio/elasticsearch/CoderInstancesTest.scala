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

package com.spotify.scio.elasticsearch

import org.elasticsearch.action.DocWriteRequest
import org.elasticsearch.action.delete.DeleteRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.support.IndicesOptions
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.xcontent.XContentType
import org.elasticsearch.index.VersionType
import org.scalactic.Equality
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

class CoderInstancesTest extends AnyFlatSpec with Matchers with CoderInstances {
  import com.spotify.scio.testing.CoderAssertions._

  val indexRequest: IndexRequest = new IndexRequest("index").id("id")
  val deleteRequest: DeleteRequest = new DeleteRequest("index2", "id2").version(2)
  val updateRequest: UpdateRequest =
    new UpdateRequest("index3", "id3").doc(Map("key" -> 4).asJava, XContentType.JSON)
  val fooRequest: FooDocWriteRequest = FooDocWriteRequest("index3", "id3")

  // DocWriteRequests don't have a well-defined equals method it seems
  implicit def indexRequestEq[T <: DocWriteRequest[_]]: Equality[T] = new Equality[T] {
    override def areEqual(a: T, b: Any): Boolean = (a, b) match {
      case (a: IndexRequest, b: IndexRequest) =>
        a.id() == b.id() && a.index() == b.index()
      case (a: UpdateRequest, b: UpdateRequest) =>
        a.id() == b.id() && a.doc().source() == b.doc().source() && a.index() == b.index()
      case (a: DeleteRequest, b: DeleteRequest) =>
        a.id() == b.id() && a.index() == b.index()
      case (a: FooDocWriteRequest, b: FooDocWriteRequest) =>
        a.id == b.id && a._index == b._index
      case _ => false
    }
  }

  "IndexRequest coders" should "work" in {
    indexRequest coderShould roundtrip()
  }

  "DeleteRequest coders" should "work" in {
    deleteRequest coderShould roundtrip()
  }

  "UpdateRequest coders" should "work" in {
    updateRequest coderShould roundtrip()
  }

  "FooDocWriteRequest coders" should "work" in {
    fooRequest coderShould roundtrip()
  }
}

case class FooDocWriteRequest(_index: String, id: String) extends DocWriteRequest[String] {
  override def index(index: String): String = index

  override def index(): String = _index

  override def `type`(`type`: String): String = `type`

  override def `type`(): String = "type"

  override def defaultTypeIfNull(defaultType: String): String = defaultType

  override def indicesOptions(): IndicesOptions = null

  override def routing(routing: String): String = routing

  override def routing(): String = "routing"

  override def version(): Long = 1

  override def version(version: Long): String = "1"

  override def versionType(): VersionType = VersionType.INTERNAL

  override def versionType(versionType: VersionType): String = versionType.toString

  override def setIfSeqNo(seqNo: Long): String = "true"

  override def setIfPrimaryTerm(term: Long): String = "true"

  override def ifSeqNo(): Long = 2L

  override def ifPrimaryTerm(): Long = 3L

  override def opType(): DocWriteRequest.OpType = DocWriteRequest.OpType.CREATE

  override def indices(): Array[String] = Array("foo")

  override def ramBytesUsed(): Long = _index.size.toLong + id.size.toLong

  override def isRequireAlias(): Boolean = false
}
