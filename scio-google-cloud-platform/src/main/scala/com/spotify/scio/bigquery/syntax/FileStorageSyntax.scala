/*
 * Copyright 2023 Spotify AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.spotify.scio.bigquery.syntax

import com.spotify.scio.bigquery.TableRow
import com.spotify.scio.io.FileStorage
import com.spotify.scio.util.ScioUtil

final class FileStorageOps(private val self: FileStorage) extends AnyVal {
  def tableRowJsonFile: Iterator[TableRow] =
    self.textFile.map(e => ScioUtil.jsonFactory.fromString(e, classOf[TableRow]))
}

trait FileStorageSyntax {
  implicit private[scio] def bigqueryFileStorageFunctions(s: FileStorage): FileStorageOps =
    new FileStorageOps(s)
}
