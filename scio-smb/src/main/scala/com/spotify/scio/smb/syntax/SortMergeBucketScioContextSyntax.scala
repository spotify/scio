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

package com.spotify.scio.smb.syntax

import com.spotify.scio.ScioContext
import com.spotify.scio.avro.{GenericRecordIO, SpecificRecordIO}
import com.spotify.scio.coders.Coder
import com.spotify.scio.smb.io.SortMergeBucketRead
import com.spotify.scio.testing.TestDataManager
import com.spotify.scio.util.ScioUtil
import com.spotify.scio.values._
import org.apache.avro.generic.GenericRecord
import org.apache.avro.specific.SpecificRecordBase
import org.apache.beam.sdk.extensions.smb.{AvroSortedBucketIO, SortedBucketIO}
import org.apache.beam.sdk.values.TupleTag

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

trait SortMergeBucketScioContextSyntax {
  implicit def asSMBScioContext(sc: ScioContext): SortedBucketScioContext =
    new SortedBucketScioContext(sc)

  // Implicits converting between supported Scio IOs and the SMB Java API
  implicit def fromSRAvro[T <: SpecificRecordBase: ClassTag: Coder](
    io: SpecificRecordIO[T]
  ): SortedBucketIO.Read[T] = {
    val path = stripFilepattern(io.path)
    AvroSortedBucketIO.read[T](new TupleTag[T](io.path), ScioUtil.classOf[T]).from(path)
  }

  implicit def fromGRAvro(io: GenericRecordIO): SortedBucketIO.Read[GenericRecord] = {
    val path = stripFilepattern(io.path)
    AvroSortedBucketIO.read(new TupleTag[GenericRecord](io.path), io.schema).from(path)
  }

  // Can't use Beam's FileSystems API in init since file might not exist yet (i.e. in test context)
  private def stripFilepattern(path: String): String = {
    val globPattern = "^(.*)/(.*)$".r

    path match {
      case globPattern(dir, _) => dir
      case _                   => path
    }
  }
}

final class SortedBucketScioContext(private val self: ScioContext) {
  def sortMergeJoin[K: Coder, L: Coder, R: Coder](
    keyClass: Class[K],
    lhs: SortedBucketIO.Read[L],
    rhs: SortedBucketIO.Read[R]
  ): SCollection[(K, (L, R))] =
    sortMergeCoGroup(keyClass, lhs, rhs)
      .flatMap {
        case (k, (l, r)) =>
          for {
            i <- l
            j <- r
          } yield (k, (i, j))
      }

  // @Todo: expand signatures in the style of multijoin.py
  def sortMergeCoGroup[K: Coder, A: Coder, B: Coder](
    keyClass: Class[K],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B]
  ): SCollection[(K, (Iterable[A], Iterable[B]))] = {
    if (self.isTest) {
      val testInputs = TestDataManager.getInput(self.testId.get)
      val (readA, readB) = (
        testInputs(SortMergeBucketRead[K, A](a.toBucketedInput.getTupleTag.getId))
          .toSCollection(self),
        testInputs(SortMergeBucketRead[K, B](b.toBucketedInput.getTupleTag.getId))
          .toSCollection(self)
      )

      readA.cogroup(readB)
    } else {
      self
        .wrap(
          self.applyInternal(
            SortedBucketIO.read(keyClass).of(a).and(b)
          )
        )
        .map { kv =>
          val cgbkResult = kv.getValue

          (
            kv.getKey,
            (
              cgbkResult.getAll(a.toBucketedInput.getTupleTag).asScala,
              cgbkResult.getAll(b.toBucketedInput.getTupleTag).asScala
            )
          )
        }
    }
  }

  def sortMergeCoGroup[K: Coder, A: Coder, B: Coder, C: Coder](
    keyClass: Class[K],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C]
  ): SCollection[(K, (Iterable[A], Iterable[B], Iterable[C]))] = {
    if (self.isTest) {
      val testInputs = TestDataManager.getInput(self.testId.get)
      val (readA, readB, readC) = (
        testInputs(SortMergeBucketRead[K, A](a.toBucketedInput.getTupleTag.getId))
          .toSCollection(self),
        testInputs(SortMergeBucketRead[K, B](b.toBucketedInput.getTupleTag.getId))
          .toSCollection(self),
        testInputs(SortMergeBucketRead[K, C](c.toBucketedInput.getTupleTag.getId))
          .toSCollection(self)
      )

      readA.cogroup(readB, readC)
    } else {
      self
        .wrap(
          self.applyInternal(
            SortedBucketIO.read(keyClass).of(a).and(b).and(c)
          )
        )
        .map { kv =>
          val cgbkResult = kv.getValue

          (
            kv.getKey,
            (
              cgbkResult.getAll(a.toBucketedInput.getTupleTag).asScala,
              cgbkResult.getAll(b.toBucketedInput.getTupleTag).asScala,
              cgbkResult.getAll(c.toBucketedInput.getTupleTag).asScala
            )
          )
        }
    }
  }
}
