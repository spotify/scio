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

import java.io.InputStream
import java.net.URL
import java.nio.channels.Channels
import java.nio.file.{Path, Paths}

import com.spotify.scio.CoreSysProps
import com.spotify.scio.avro.AvroSysProps
import com.spotify.scio.avro.types.MacroUtil._
import org.apache.avro.Schema
import org.apache.avro.Schema.Type._
import org.apache.avro.file.DataFileStream
import org.apache.avro.generic.GenericDatumReader
import org.apache.beam.sdk.io.FileSystems
import org.apache.beam.sdk.io.fs.MatchResult.Status
import org.apache.beam.sdk.io.fs.{MatchResult, ResourceId}
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Charsets
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.hash.Hashing
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.Files
import org.slf4j.LoggerFactory

import scala.jdk.CollectionConverters._
import scala.reflect.macros._
import scala.util.Try

private[types] object TypeProvider {
  private val logger = LoggerFactory.getLogger(this.getClass)

  // In order to use FileSystems functions we first need to register all FileSystemRegistrars
  // located on our class path.
  registerFileSystemRegistrars()

  def schemaImpl(c: blackbox.Context)(annottees: c.Expr[Any]*): c.Expr[Any] = {
    val schemaString = extractStrings(c, "Missing schema").head
    val schema = new Schema.Parser().parse(schemaString)
    schemaToType(c)(schema, annottees)
  }

  def schemaFileImpl(c: blackbox.Context)(annottees: c.Expr[Any]*): c.Expr[Any] = {
    val file = extractStrings(c, "Missing file").head.trim.replaceAll("\n", "")
    val fileInputStream = Try(readFromFileSystem(file))
      .recover { case _: Throwable => readFromUrl(file) }
    if (fileInputStream.isFailure) {
      throw new RuntimeException(s"Error reading schema file $file")
    }
    val schema = new Schema.Parser().parse(fileInputStream.get)
    schemaToType(c)(schema, annottees)
  }

  private def readFromFileSystem(file: String): InputStream = {
    val files = FileSystems.`match`(file)
    assume(
      files.metadata().size() == 1,
      s"File argument '$file' must match exactly one file. " +
        s"We've matched ${files.metadata().size()} files."
    )
    Channels.newInputStream(FileSystems.open(files.metadata().get(0).resourceId()))
  }

  private def readFromUrl(file: String): InputStream =
    new URL(file).openStream()

  def pathImpl(c: blackbox.Context)(annottees: c.Expr[Any]*): c.Expr[Any] = {
    val path = extractStrings(c, "Missing path").head
    val schema = schemaFromGcsFolder(path)
    schemaToType(c)(schema, annottees)
  }

  private def schemaFromGcsFolder(path: String): Schema = {
    val p = path.trim.replaceAll("\n", "")
    emitWarningIfGcsGlobPath(p)

    val avroFile = {
      def matchResult(r: MatchResult): Option[ResourceId] =
        if (r.status() != Status.OK || r.metadata().isEmpty) {
          None
        } else {
          val last = r.metadata().asScala.maxBy(_.resourceId().toString)
          if (last.sizeBytes() > 0) Some(last.resourceId()) else None
        }
      val r = matchResult(FileSystems.`match`(p)) match {
        case Some(x) => Some(x)
        case None =>
          matchResult(FileSystems.`match`(p.replaceFirst("/?$", "/*.avro")))
      }
      require(r.isDefined, s"Unable to match Avro file from path '$p'")
      r.get
    }

    logger.info(s"Reading Avro schema from file '$avroFile'")

    var reader: DataFileStream[Void] = null
    try {
      reader = new DataFileStream(
        Channels.newInputStream(FileSystems.open(avroFile)),
        new GenericDatumReader[Void]()
      )
      reader.getSchema
    } finally {
      if (reader != null) {
        reader.close()
      }
    }
  }

  private def emitWarningIfGcsGlobPath(path: String) = {
    val gcsGlobPathPattern = "(gs://[^\\[*?]*)[\\[*?].*".r
    path match {
      case gcsGlobPathPattern(pathPrefix) =>
        logger.warn(
          "Matching GCS wildcards may be inefficient if there are many files that " +
            s"share the prefix '$pathPrefix'."
        )
        logger.warn(
          s"Macro expansion will be slow and might not even finish before hitting " +
            "compiler GC limit."
        )
        logger.warn("Consider using a more specific path glob.")
      case _ =>
    }
  }

  def toSchemaImpl(c: blackbox.Context)(annottees: c.Expr[Any]*): c.Expr[Any] = {
    import c.universe._
    checkMacroEnclosed(c)

    val r = annottees.map(_.tree) match {
      case l @ List(
            q"$mods class $name[..$_] $_(..$fields) extends { ..$_ } with ..$parents { $_ => ..$body }"
          ) if mods.asInstanceOf[Modifiers].hasFlag(Flag.CASE) =>
        if (parents.map(_.toString()).toSet != Set("scala.Product", "scala.Serializable")) {
          c.abort(c.enclosingPosition, s"Invalid annotation, don't extend the case class $l")
        }
        val docs = getRecordDocs(c)(l)
        val docMethod = docs.headOption
          .map(d => q"override def doc: _root_.java.lang.String = $d")
          .toSeq
        val docTrait = docMethod.map(_ => tq"${p(c, ScioAvroType)}.HasAvroDoc")

        val fnTrait = if (fields.size <= 22) {
          Seq(tq"${TypeName(s"Function${fields.size}")}[..${fields.map(_.children.head)}, $name]")
        } else {
          Seq()
        }

        val schemaMethod = Seq(q"""override def schema: ${p(c, ApacheAvro)}.Schema =
                 ${p(c, ScioAvroType)}.schemaOf[$name]""")

        val caseClassTree = q"""${caseClass(c)(mods, name, fields, body)}"""

        if (shouldDumpClassesForPlugin) {
          dumpCodeForScalaPlugin(c)(Seq.empty, caseClassTree, name.toString)
        }

        q"""$caseClassTree
            ${companion(c)(
          name,
          docTrait ++ fnTrait,
          schemaMethod ++ docMethod,
          fields.asInstanceOf[Seq[c.Tree]]
        )}
        """
      case t =>
        val error =
          s"""Invalid annotation:
             |
             |Refer to https://spotify.github.io/scio/api/com/spotify/scio/avro/types/AvroType$$$$toSchema.html
             |for details on how to use `@AvroType.toSchema`
             |
             |>> $t
          """.stripMargin

        c.abort(c.enclosingPosition, error)
    }

    c.Expr[Any](r)
  }

  private def schemaToType(
    c: blackbox.Context
  )(schema: Schema, annottees: Seq[c.Expr[Any]]): c.Expr[Any] = {
    import c.universe._
    checkMacroEnclosed(c)

    // Returns: (raw type, e.g. Int, String, NestedRecord, nested case class definitions)
    def getField(className: String, fieldName: String, fieldSchema: Schema): (Tree, Seq[Tree]) =
      fieldSchema.getType match {
        case UNION =>
          val unionTypes = fieldSchema.getTypes.asScala.map(_.getType).distinct
          if (unionTypes.size != 2 || !unionTypes.contains(NULL)) {
            c.abort(
              c.enclosingPosition,
              s"type: ${fieldSchema.getType} is not supported. " +
                s"Union type needs to contain exactly one 'null' type and one non null type."
            )
          }
          val (field, recordClasses) =
            getField(
              className,
              fieldName,
              fieldSchema.getTypes.asScala.filter(_.getType != NULL).head
            )
          (tq"_root_.scala.Option[$field]", recordClasses)
        case BOOLEAN =>
          (tq"_root_.scala.Boolean", Nil)
        case LONG =>
          (tq"_root_.scala.Long", Nil)
        case DOUBLE =>
          (tq"_root_.scala.Double", Nil)
        case INT =>
          (tq"_root_.scala.Int", Nil)
        case FLOAT =>
          (tq"_root_.scala.Float", Nil)
        case STRING | ENUM =>
          (tq"_root_.java.lang.String", Nil)
        case BYTES =>
          (tq"_root_.com.google.protobuf.ByteString", Nil)
        case ARRAY =>
          val (field, generatedCaseClasses) =
            getField(className, fieldName, fieldSchema.getElementType)
          (tq"_root_.scala.List[$field]", generatedCaseClasses)
        case MAP =>
          val (fieldType, recordCaseClasses) =
            getField(className, fieldName, fieldSchema.getValueType)
          (tq"_root_.scala.collection.Map[_root_.java.lang.String,$fieldType]", recordCaseClasses)
        case RECORD =>
          val nestedClassName = s"$className$$${fieldSchema.getName}"
          val (fields, recordClasses) =
            extractFields(nestedClassName, fieldSchema)
          (
            q"${Ident(TypeName(nestedClassName))}",
            Seq(q"case class ${TypeName(nestedClassName)}(..$fields)") ++ recordClasses
          )
        case t =>
          c.abort(c.enclosingPosition, s"type: $t not supported")
      }

    // Returns: ("fieldName: fieldType", nested case class definitions)
    def extractField(
      className: String,
      fieldName: String,
      fieldSchema: Schema
    ): (Tree, Seq[Tree]) = {
      val (fieldType, recordClasses) =
        getField(className, SchemaUtil.unescapeNameIfReserved(fieldName), fieldSchema)
      fieldSchema.getType match {
        case UNION =>
          (q"val ${TermName(fieldName)}: $fieldType = None", recordClasses)
        case _ =>
          (q"${TermName(fieldName)}: $fieldType", recordClasses)
      }
    }

    def extractFields(className: String, schema: Schema): (Seq[Tree], Seq[Tree]) = {
      val f = schema.getFields.asScala
        .map(f => extractField(className, f.name, f.schema))

      val fields = f.map(_._1)

      val recordClasses = f
        .flatMap(_._2)
        .groupBy(_.asInstanceOf[ClassDef].name)
        // note that if there are conflicting definitions of a nested record type, the Avro schema
        // parser itself will catch it before getting to this step.
        .map { case (_, cDefs) => cDefs.head } // Don't generate duplicate case classes

      (fields.toSeq, recordClasses.toSeq)
    }

    val r = annottees.map(_.tree) match {
      case l @ List(
            q"$mods class $name[..$_] $_(..$cfields) extends { ..$_ } with ..$parents { $_ => ..$_ }"
          ) if mods.asInstanceOf[Modifiers].flags == NoFlags =>
        if (parents.map(_.toString()).toSet != Set("scala.AnyRef")) {
          c.abort(c.enclosingPosition, s"Invalid annotation, don't extend the case class $l")
        }
        if (cfields.nonEmpty) {
          c.abort(c.enclosingPosition, s"Invalid annotation, don't provide class fields $l")
        }

        val (fields, recordClasses) = extractFields(name.toString, schema)

        val docs = getRecordDocs(c)(l)
        val docMethod = docs.headOption
          .map(d => q"override def doc: _root_.java.lang.String = $d")
          .toSeq
        val docTrait = docMethod
          .map(_ => tq"${p(c, ScioAvroType)}.HasAvroDoc")

        val schemaMethod = Seq(q"""override def schema: ${p(c, ApacheAvro)}.Schema =
                 new ${p(c, ApacheAvro)}.Schema.Parser().parse(${schema.toString})""")

        val caseClassTree = q"${caseClass(c)(mods, name, fields, Nil)}"

        if (shouldDumpClassesForPlugin) {
          dumpCodeForScalaPlugin(c)(recordClasses, caseClassTree, name.toString())
        }

        q"""$caseClassTree
           ${companion(c)(name, docTrait, schemaMethod ++ docMethod, fields)}
           ..$recordClasses
         """
      case t => c.abort(c.enclosingPosition, s"Invalid annotation $t")
    }

    c.Expr[Any](r)
  }

  /** Generate a case class. */
  private def caseClass(
    c: blackbox.Context
  )(mods: c.Modifiers, name: c.TypeName, fields: Seq[c.Tree], body: Seq[c.Tree]): c.Tree = {
    import c.universe._
    val caseMods = Modifiers(Flag.CASE, typeNames.EMPTY, mods.annotations)
    q"$caseMods class $name(..$fields) extends ${p(c, ScioAvroType)}.HasAvroAnnotation { ..$body }"
  }

  /** Generate a companion object. */
  private def companion(
    c: blackbox.Context
  )(name: c.TypeName, traits: Seq[c.Tree], methods: Seq[c.Tree], fields: Seq[c.Tree]): c.Tree = {
    import c.universe._

    val tupledMethod =
      if (fields.size > 1 && fields.size <= 22) {
        val overrideFlag =
          if (traits.exists(_.toString().contains("Function"))) Flag.OVERRIDE
          else NoFlags
        Seq(q"$overrideFlag def tupled = (${TermName(name.toString)}.apply _).tupled")
      } else {
        Seq()
      }

    q"""object ${TermName(name.toString)} extends ${p(c, ScioAvroType)}.HasAvroSchema[$name] with ..$traits {
          override def toPrettyString(indent: Int = 0): String =
            ${p(c, s"$ScioAvro.types.SchemaUtil")}.toPrettyString(this.getClass.getName, this.schema, indent)
          override def fromGenericRecord: (${p(c, ApacheAvro)}.generic.GenericRecord => $name) =
            ${p(c, ScioAvroType)}.fromGenericRecord[$name]
          override def toGenericRecord: ($name => ${p(c, ApacheAvro)}.generic.GenericRecord) =
            ${p(c, ScioAvroType)}.toGenericRecord[$name]
          ..$tupledMethod
          ..$methods
        }
     """
  }

  /** Extract string from annotation. */
  private def extractStrings(c: blackbox.Context, errorMessage: String): List[String] = {
    import c.universe._

    def str(tree: c.Tree) = tree match {
      // "string literal"
      case Literal(Constant(s: String)) => s
      // "string literal".stripMargin
      case Select(Literal(Constant(s: String)), TermName("stripMargin")) =>
        s.stripMargin
      case _ => c.abort(c.enclosingPosition, errorMessage)
    }

    c.macroApplication match {
      case Apply(Select(Apply(_, xs: List[_]), _), _) =>
        val args = xs.map(str)
        if (args.isEmpty) {
          c.abort(c.enclosingPosition, errorMessage)
        }
        args
      case _ => c.abort(c.enclosingPosition, errorMessage)
    }
  }

  /** Enforce that the macro is not enclosed by a package, but a class or object instead. */
  private def checkMacroEnclosed(c: blackbox.Context): Unit =
    if (!c.internal.enclosingOwner.isClass) {
      c.abort(c.enclosingPosition, s"@AvroType declaration must be inside a class or object.")
    }

  private def getRecordDocs(
    c: blackbox.Context
  )(tree: Seq[c.universe.Tree]): List[c.universe.Tree] = {
    import c.universe._
    tree.head
      .asInstanceOf[ClassDef]
      .mods
      .annotations
      .filter(_.children.head.toString().matches("^new doc"))
      .map(_.children.tail.head)
  }

  /**
   * Check if compiler should dump generated code for Scio IDEA plugin.
   *
   * This is used to mitigate lack of support for Scala macros in IntelliJ.
   */
  private def shouldDumpClassesForPlugin =
    !AvroSysProps.DisableDump.value(default = "false").toBoolean

  private def getBQClassCacheDir: Path =
    // TODO: add this as key/value settings with default etc
    AvroSysProps.ClassCacheDirectory.valueOption.map(Paths.get(_)).getOrElse {
      Paths
        .get(CoreSysProps.TmpDir.value)
        .resolve(CoreSysProps.User.value)
        .resolve("generated-classes")
    }

  private def pShowCode(
    c: blackbox.Context
  )(records: Seq[c.Tree], caseClass: c.Tree): Seq[String] = {
    // print only records and case class and do it nicely so that we can just inject those
    // in scala plugin.
    import c.universe._
    (Seq(caseClass) ++ records).map {
      case q"case class $name(..$fields) { ..$_ }" =>
        s"case class $name(${fields
          .map { case ValDef(_, fname, ftpt, _) =>
            s"${SchemaUtil.escapeNameIfReserved(fname.toString)} : $ftpt"
          }
          .mkString(", ")})"
      case q"case class $name(..$fields) extends $annotation { ..$_ }" =>
        s"case class $name(${fields
          .map { case ValDef(_, fname, ftpt, _) =>
            s"${SchemaUtil.escapeNameIfReserved(fname.toString)} : $ftpt"
          }
          .mkString(", ")}) extends $annotation"
      case _ => ""
    }
  }

  private def genHashForMacro(owner: String, srcFile: String): String =
    Hashing
      .murmur3_32()
      .newHasher()
      .putString(owner, Charsets.UTF_8)
      .putString(srcFile, Charsets.UTF_8)
      .hash()
      .toString

  private def dumpCodeForScalaPlugin(
    c: blackbox.Context
  )(records: Seq[c.universe.Tree], caseClassTree: c.universe.Tree, name: String): Unit = {
    val owner = c.internal.enclosingOwner.fullName
    val srcFile = c.macroApplication.pos.source.file.canonicalPath
    val hash = genHashForMacro(owner, srcFile)

    val prettyCode = pShowCode(c)(records, caseClassTree).mkString("\n")
    val genSrcFile = getBQClassCacheDir.resolve(s"$name-$hash.scala").toFile

    logger.debug(s"Will dump generated $name of $owner from $srcFile to $genSrcFile")

    Files.createParentDirs(genSrcFile)
    Files.asCharSink(genSrcFile, Charsets.UTF_8).write(prettyCode)
  }

  private def registerFileSystemRegistrars(): Unit = {
    // In order to find all the FileSystemRegistrars on the path we need to change
    // ContextClassLoader to be the same as our ClassLoader.
    java.lang.Thread
      .currentThread()
      .setContextClassLoader(getClass.getClassLoader)
    FileSystems.setDefaultPipelineOptions(PipelineOptionsFactory.create())
  }
}
