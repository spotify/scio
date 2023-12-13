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

package com.spotify.scio.io

import com.spotify.scio.util.ScioUtil
import org.apache.beam.sdk.io.FileSystems
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment
import org.apache.beam.sdk.io.fs.MatchResult.Metadata
import org.apache.commons.compress.compressors.CompressorStreamFactory
import org.apache.commons.io.IOUtils

import java.io._
import java.nio.channels.Channels
import java.nio.charset.StandardCharsets
import java.util.Collections
import scala.collection.compat._
import scala.jdk.CollectionConverters._
import scala.util.Try

private[scio] object FileStorage {

  private val ShardPattern = "(.*)-(\\d+)-of-(\\d+)(.*)".r

  @inline final def apply(path: String, suffix: String): FileStorage = new FileStorage(path, suffix)

  def listFiles(path: String, suffix: String): Seq[Metadata] =
    FileSystems
      .`match`(ScioUtil.filePattern(path, suffix), EmptyMatchTreatment.DISALLOW)
      .metadata()
      .iterator
      .asScala
      .toSeq
}

final private[scio] class FileStorage(path: String, suffix: String) {

  import FileStorage._

  private def listFiles: Seq[Metadata] = FileStorage.listFiles(path, suffix)

  private def getObjectInputStream(meta: Metadata): InputStream =
    Channels.newInputStream(FileSystems.open(meta.resourceId()))

  def textFile: Iterator[String] = {
    val factory = new CompressorStreamFactory()
    def wrapInputStream(in: InputStream) = {
      val buffered = new BufferedInputStream(in)
      try {
        factory.createCompressorInputStream(buffered)
      } catch {
        case _: Throwable => buffered
      }
    }
    val input = getDirectoryInputStream(wrapInputStream)
    IOUtils.lineIterator(input, StandardCharsets.UTF_8).asScala
  }

  def isDone(): Boolean = {
    val files = Try(listFiles).recover { case _: FileNotFoundException => Seq.empty }.get

    // best effort matching shardNumber and numShards
    // relies on the shardNameTemplate to be of '$prefix-$shardNumber-of-$numShards$suffix' format
    val writtenShards = files
      .map(_.resourceId().toString)
      .flatMap {
        case ShardPattern(prefix, shardNumber, numShards, suffix) =>
          val part = for {
            idx <- Try(shardNumber.toInt)
            total <- Try(numShards.toInt)
            // prefix or suffix may contain pane/window info
            key = (prefix, suffix, total)
          } yield key -> idx
          part.toOption
        case _ =>
          None
      }
      .groupMap(_._1)(_._2)

    if (files.isEmpty) {
      // no file matched
      false
    } else if (writtenShards.isEmpty) {
      // assume progress is complete when shard info is not retrieved and files are present
      true
    } else {
      // we managed to get shard info, verify that all of them were written
      writtenShards.forall { case ((_, _, total), idxs) => idxs.size == total }
    }
  }

  private[scio] def getDirectoryInputStream(
    wrapperFn: InputStream => InputStream = identity
  ): InputStream = {
    val inputs = listFiles.map(getObjectInputStream).map(wrapperFn).asJava
    new SequenceInputStream(Collections.enumeration(inputs))
  }
}
