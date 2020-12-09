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

package com.spotify.scio.avro.types

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord

import scala.annotation.{compileTimeOnly, StaticAnnotation}
import scala.reflect.runtime.universe._

/**
 * Macro annotations and converter generators for Avro types.
 *
 * The following table lists Avro types and their Scala counterparts.
 * {{{
 * Avro type      Scala type
 * BOOLEAN        Boolean
 * LONG           Long
 * INT            Int
 * DOUBLE         Double
 * FLOAT          Float
 * STRING, ENUM   String
 * BYTES          com.google.protobuf.ByteString
 * ARRAY          List[T]
 * MAP            Map[String, T]
 * UNION          Option[T]
 * RECORD         Nested case class
 * }}}
 *
 * @groupname trait Traits for annotated types
 * @groupname annotation Type annotations
 * @groupname converters Converters
 * @groupname Ungrouped Other Members
 */
trait AvroTypeMacros {

  /**
   * Macro annotation for an Avro schema.
   *
   * Generate case classes for an Avro schema. Note that `schema` must be a single string literal
   * of the JSON schema with optional `.stripMargin` at the end. For example:
   *
   * {{{
   *   @AvroType.fromSchema(
   *       """
   *         |{
   *         |  "type": "record",
   *         |  "namespace": "com.spotify.namespace",
   *         |  "name": "RecordName",
   *         |  "fields": [
   *         |    { "name": "boolF", "type": "boolean"},
   *         |    { "name": "intF", "type": "int"},
   *         |    { "name": "longF", "type": "long"},
   *         |    { "name": "floatF", "type": "float"},
   *         |    { "name": "doubleF", "type": "double"},
   *         |    { "name": "stringF", "type": "string"},
   *         |    { "name": "byteStringF", "type": "bytes"}
   *         |  ]
   *         |}
   *       """.stripMargin)
   *   class MyRecord
   * }}}
   *
   * Also generate a companion object with convenience methods.
   * @group annotation
   */
  @compileTimeOnly("")
  class fromSchema(schema: String) extends StaticAnnotation {
    def macroTransform(annottees: Any*): Any = ???
  }

  /**
   * Macro annotation for a path containing Avro files.
   *
   * Generates case classes from a path which contains Avro files.
   * Path needs to represent a folder, hence it always needs to end with `/`.
   * Inside of the folder needs to exist at least one file matching `*.avro` glob.
   *
   * Note that path must be a single string literal with optional `.stripMargin` at the end.
   * For example:
   *
   * {{{
   * @AvroType.fromPath("gs://myBucket/myFolder/")
   * class MyRecord
   * }}}
   *
   * or
   *
   * {{{
   * @AvroType.fromPath(
   *    """
   *     | gs://myBucket/myFolder/
   *     | myLooooooooooooooooongPath/
   *    """.stripMargin)
   * class MyRecord
   * }}}
   *
   * Globs are supported as a part of the path. For example:
   *
   * {{{
   * @AvroType.fromPath("gs://myBucket{@literal /}*{@literal /}*{@literal /}*{@literal /}")
   * class MyRecord
   * }}}
   *
   * Also generate a companion object with convenience methods.
   * @group annotation
   */
  @compileTimeOnly("")
  class fromPath(folderGlob: String) extends StaticAnnotation {
    def macroTransform(annottees: Any*): Any = ???
  }

  /**
   * Macro annotation for a file which contains Avro schema.
   *
   * Generate case classes for an Avro schema. File can be either local or remote files.
   * For example file can be located on Google Cloud Storage (GCS):
   *
   * {{{
   *   @AvroType.fromSchemaFile("gs://myBucket/myFolder/schema-file.avsc")
   *   class MyRecord
   * }}}
   *
   * For local files, you need to either provide absolute path,
   * or path relative to project root directory. For example:
   *
   * {{{
   *   @AvroType.fromSchemaFile("sub-project/src/main/avro/schema-file.avsc")
   *   class MyRecord
   * }}}
   *
   * Also generate a companion object with convenience methods.
   * @group annotation
   */
  @compileTimeOnly("")
  class fromSchemaFile(schemaFile: String) extends StaticAnnotation {
    def macroTransform(annottees: Any*): Any = ???
  }

  /**
   * Macro annotation for case classes to be saved to Avro files.
   *
   * Note that this annotation does not generate case classes, only a companion object with
   * convenience methods. You need to define a complete case class for as output record. For
   * example:
   *
   * {{{
   * @AvroType.toSchema
   * case class Result(name: Option[String] = None, score: Option[Double] = None)
   * }}}
   *
   * It is recommended that you define all of your fields as Option.
   * This way you could stop populating them in the future if you notice that you don't need them.
   *
   * This macro doesn't help you with schema evolution.
   * It's up to you to follow the best practices on how to do evolution of your Avro schemas.
   * Rule of thumb is to only add new fields, without removing the old ones.
   * @group annotation
   */
  @compileTimeOnly("")
  class toSchema extends StaticAnnotation {
    def macroTransform(annottees: Any*): Any = ???
  }

  /** Generate [[org.apache.avro.Schema Schema]] for a case class. */
  def schemaOf[T: TypeTag]: Schema = ???

  /**
   * Generate a converter function from [[org.apache.avro.generic.GenericRecord GenericRecord]]
   * to the given case class `T`.
   * @group converters
   */
  def fromGenericRecord[T]: GenericRecord => T = ???

  /**
   * Generate a converter function from the given case class `T` to
   * [[org.apache.avro.generic.GenericRecord GenericRecord]].
   * @group converters
   */
  def toGenericRecord[T]: T => GenericRecord = ???
}
