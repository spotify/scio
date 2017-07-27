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

package org.apache.beam.sdk.io

import java.nio.channels.ReadableByteChannel
import java.util.Collections
import java.util.regex.Pattern

import org.apache.beam.sdk.extensions.gcp.storage.GcsFileSystemRegistrar
import org.apache.beam.sdk.io.fs.{MatchResult, ResourceId}
import org.apache.beam.sdk.options.PipelineOptionsFactory

import scala.collection.JavaConverters._

// FIXME: figure out how to add class paths at compile time to avoid registering these manually
/** Workaround for FileSystemRegistrar class paths not available in during macro compilation. */
object FileSystemsUtil {

  private val FILE_SCHEME_PATTERN = Pattern.compile("(?<scheme>[a-zA-Z][-a-zA-Z0-9+.]*):.*")

  private lazy val fileSystems: Map[String, FileSystem[_]] = {
    val options = PipelineOptionsFactory.create()
    val registrars = Set(new LocalFileSystemRegistrar, new GcsFileSystemRegistrar)
    FileSystems.verifySchemesAreUnique(options, registrars.asJava).asScala.toMap
  }

  // scalastyle:off method.name
  def `match`(spec: String): MatchResult = {
    val matches = getFileSystem(parseScheme(spec)).`match`(Collections.singletonList(spec))
    assert(
      matches.size() == 1,
      s"FileSystem implementation for $spec did not return exactly one MatchResult: $matches")
    matches.get(0)
  }
  // scalastyle:on regex

  def open(resourceId: ResourceId): ReadableByteChannel =
    getFileSystem(resourceId.getScheme).open(resourceId)

  private def getFileSystem(scheme: String): FileSystem[ResourceId] = {
    val fs = fileSystems.get(scheme)
    require(fs.isDefined, "Unable to find registrar for " + scheme)
    fs.get.asInstanceOf[FileSystem[ResourceId]]
  }

  private def parseScheme(spec: String): String = {
    val matcher = FILE_SCHEME_PATTERN.matcher(spec)
    if (!matcher.matches()) "file" else matcher.group("scheme").toLowerCase()
  }

}
