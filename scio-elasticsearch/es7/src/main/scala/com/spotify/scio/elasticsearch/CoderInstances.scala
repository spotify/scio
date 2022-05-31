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

import java.io.{InputStream, OutputStream}

import com.spotify.scio.coders._
import org.apache.beam.sdk.coders.{AtomicCoder, CoderException}
import org.elasticsearch.action.DocWriteRequest
import org.elasticsearch.action.delete.DeleteRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.common.io.stream.{
  InputStreamStreamInput,
  OutputStreamStreamOutput,
  StreamInput,
  Writeable
}

trait CoderInstances {
  private val INDEX_REQ_INDEX = 0
  private val UPDATE_REQ_INDEX = 1
  private val DELETE_REQ_INDEX = 2
  private val KRYO_INDEX = 3

  private lazy val kryoCoder = Coder.kryo[DocWriteRequest[_]]
  private lazy val kryoBCoder = CoderMaterializer.beamWithDefault(kryoCoder)

  implicit val docWriteRequestCoder: Coder[DocWriteRequest[_]] =
    Coder.beam[DocWriteRequest[_]](new AtomicCoder[DocWriteRequest[_]] {
      override def encode(value: DocWriteRequest[_], outStream: OutputStream): Unit =
        value match {
          case i: IndexRequest =>
            outStream.write(INDEX_REQ_INDEX)
            indexRequestBCoder.encode(i, outStream)
          case u: UpdateRequest =>
            outStream.write(UPDATE_REQ_INDEX)
            updateRequestBCoder.encode(u, outStream)
          case d: DeleteRequest =>
            outStream.write(DELETE_REQ_INDEX)
            deleteRequestBCoder.encode(d, outStream)
          case _ =>
            outStream.write(KRYO_INDEX)
            kryoBCoder.encode(value, outStream)
        }

      override def decode(inStream: InputStream): DocWriteRequest[_] = {
        val request = inStream.read() match {
          case INDEX_REQ_INDEX  => indexRequestBCoder.decode(inStream)
          case UPDATE_REQ_INDEX => updateRequestBCoder.decode(inStream)
          case DELETE_REQ_INDEX => deleteRequestBCoder.decode(inStream)
          case KRYO_INDEX       => kryoBCoder.decode(inStream)
          case n                => throw new CoderException(s"Unknown index $n")
        }

        request.asInstanceOf[DocWriteRequest[_]]
      }
    })

  private def writableBCoder[T <: Writeable](
    constructor: StreamInput => T
  ): org.apache.beam.sdk.coders.Coder[T] = new AtomicCoder[T] {
    override def encode(value: T, outStream: OutputStream): Unit =
      value.writeTo(new OutputStreamStreamOutput(outStream))
    override def decode(inStream: InputStream): T =
      constructor(new InputStreamStreamInput(inStream))
  }

  private val indexRequestBCoder = writableBCoder[IndexRequest](new IndexRequest(_))
  implicit val indexRequestCoder: Coder[IndexRequest] =
    Coder.beam[IndexRequest](indexRequestBCoder)

  private val deleteRequestBCoder = writableBCoder[DeleteRequest](new DeleteRequest(_))
  implicit val deleteRequestCoder: Coder[DeleteRequest] =
    Coder.beam[DeleteRequest](deleteRequestBCoder)

  private val updateRequestBCoder = writableBCoder[UpdateRequest](new UpdateRequest(_))
  implicit val updateRequestCoder: Coder[UpdateRequest] =
    Coder.beam[UpdateRequest](updateRequestBCoder)
}
