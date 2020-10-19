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

package com.spotify.scio.schemas

import scala.jdk.CollectionConverters._
import org.apache.beam.sdk.schemas.{Schema => BSchema}

private[scio] object PrettyPrint {
  val header: String =
    f"""
    |┌──────────────────────────────────────────┬──────────────────────┬──────────┐
    |│ NAME                                     │ TYPE                 │ NULLABLE │
    |├──────────────────────────────────────────┼──────────────────────┼──────────┤%n""".stripMargin
      .drop(1)
  val footer: String =
    f"""
    |└──────────────────────────────────────────┴──────────────────────┴──────────┘%n""".stripMargin.trim

  private def printContent(fs: List[BSchema.Field], prefix: String = ""): String =
    fs.map { f =>
      val nullable = if (f.getType.getNullable) "YES" else "NO"
      val `type` = f.getType
      val typename =
        `type`.getTypeName match {
          case BSchema.TypeName.ARRAY =>
            s"${`type`.getCollectionElementType.getTypeName}[]"
          case BSchema.TypeName.LOGICAL_TYPE =>
            `type`.getLogicalType().getIdentifier()
          case t => t
        }
      val out =
        f"│ ${prefix + f.getName}%-40s │ $typename%-20s │ $nullable%-8s │%n"
      val underlying =
        if (f.getType.getTypeName == BSchema.TypeName.ROW)
          printContent(f.getType.getRowSchema.getFields.asScala.toList, s"$prefix${f.getName}.")
        else ""

      out + underlying
    }.mkString("")

  def prettyPrint(fs: List[BSchema.Field]): String =
    header + printContent(fs) + footer
}
