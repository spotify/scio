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

package com.spotify.scio.avro

import org.apache.avro.{Conversion, Schema}
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericRecord}
import org.apache.avro.io.{DatumReader, DatumWriter}
import org.apache.avro.specific.{
  SpecificData,
  SpecificDatumReader,
  SpecificDatumWriter,
  SpecificRecord
}
import org.apache.beam.sdk.extensions.avro.io.AvroDatumFactory

import java.lang.reflect.{InvocationHandler, InvocationTargetException, Method, Proxy}
import java.util.concurrent.ConcurrentHashMap

import scala.jdk.CollectionConverters._
import scala.util.Try
import scala.util.chaining._

/**
 * AvroDatumFactory for [[GenericRecord]] forcing underlying [[CharSequence]] implementation to
 * [[String]]
 *
 * Avro default [[CharSequence]] implementation is [[org.apache.avro.util.Utf8]] which can't be used
 * when joining or as SMB keys as it doest not implement equals
 */
private[scio] object GenericRecordDatumFactory extends AvroDatumFactory.GenericDatumFactory {

  // own GenericData instance so disabling the fast reader doesn't touch the GenericData.get() singleton
  private class ScioGenericDatumReader
      extends GenericDatumReader[GenericRecord](null, null, new GenericData()) {
    override def findStringClass(schema: Schema): Class[_] = super.findStringClass(schema) match {
      case cls if cls == classOf[CharSequence] => classOf[String]
      case cls                                 => cls
    }
  }
  override def apply(writer: Schema, reader: Schema): DatumReader[GenericRecord] = {
    val datumReader = new ScioGenericDatumReader()
    AvroCompat.disableFastReader(datumReader.getData)
    datumReader.setExpected(reader)
    datumReader.setSchema(writer)
    datumReader
  }
}

/**
 * AvroDatumFactory for [[SpecificRecord]] forcing underlying [[CharSequence]] implementation to
 * [[String]]
 *
 * Avro default [[CharSequence]] implementation is [[org.apache.avro.util.Utf8]] which can't be used
 * when joining or as SMB keys as it doest not implement equals
 */
private[scio] class SpecificRecordDatumFactory[T <: SpecificRecord](recordType: Class[T])
    extends AvroDatumFactory.SpecificDatumFactory[T](recordType) {
  import SpecificRecordDatumFactory._

  override def apply(writer: Schema): DatumWriter[T] = {
    val datumWriter = new SpecificDatumWriter(recordType)
    // avro 1.8 generated code does not add conversions to the data
    if (runtimeAvroVersion.exists(_.startsWith("1.8."))) {
      addLogicalTypeConversions(datumWriter.getData.asInstanceOf[SpecificData], writer)
    }
    datumWriter.setSchema(writer)
    datumWriter
  }

  // TODO move this to companion object
  private class ScioSpecificDatumReader extends SpecificDatumReader[T](recordType) {
    // Avro 1.12.0 bug: SpecificDatumReader(Class) chains to SpecificDatumReader(SpecificData)
    // which doesn't populate trustedPackages with SERIALIZABLE_PACKAGES defaults
    // reflection avoids breaking avro 1.8. Equivalent to:
    // getTrustedPackages.addAll(
    //   java.util.Arrays.asList(SpecificDatumReader.SERIALIZABLE_PACKAGES: _*)
    // )
    Try {
      val method = classOf[SpecificDatumReader[_]].getMethod("getTrustedPackages")
      val trusted = method.invoke(this).asInstanceOf[java.util.List[String]]
      val field = classOf[SpecificDatumReader[_]].getField("SERIALIZABLE_PACKAGES")
      val packages = field.get(null).asInstanceOf[Array[String]]
      trusted.addAll(java.util.Arrays.asList(packages: _*))
    }

    override def findStringClass(schema: Schema): Class[_] = super.findStringClass(schema) match {
      case cls if cls == classOf[CharSequence] => classOf[String]
      case cls                                 => cls
    }
  }

  override def apply(writer: Schema, reader: Schema): DatumReader[T] = {
    AvroCompat.trustClasses(recordType, reader)
    val datumReader = new ScioSpecificDatumReader()
    // avro 1.8 generated code does not add conversions to the data
    if (runtimeAvroVersion.exists(_.startsWith("1.8."))) {
      addLogicalTypeConversions(datumReader.getData.asInstanceOf[SpecificData], reader)
    }
    // the data is the record class model, shared with other readers of the same class
    AvroCompat.disableFastReader(datumReader.getData)
    datumReader.setExpected(reader)
    datumReader.setSchema(writer)
    datumReader
  }
}

private[scio] object SpecificRecordDatumFactory {

  @transient private lazy val runtimeAvroVersion: Option[String] =
    Option(classOf[Schema].getPackage.getImplementationVersion)

  private def addLogicalTypeConversions[T <: SpecificRecord](
    data: SpecificData,
    schema: Schema,
    seenSchemas: Set[Schema] = Set.empty
  ): Unit = {
    if (seenSchemas.contains(schema)) {
      return
    }

    schema.getType match {
      case Schema.Type.RECORD =>
        // avro 1.8 patching
        //   - specific data must find the class
        //   - class must have a 'conversions' field
        //   - 'conversion' field must be a static array of Conversion[_]
        //   - add non null conversions to the data
        for {
          clazz <- Option(data.getClass(schema))
          field <- Try(clazz.getDeclaredField("conversions").tap(_.setAccessible(true))).toOption
          conversions <- Try(field.get(null)).collect { case c: Array[Conversion[_]] => c }.toOption
        } yield conversions.filter(_ != null).foreach(data.addLogicalTypeConversion)

        val updatedSeenSchemas = seenSchemas + schema
        schema.getFields.asScala.foreach { f =>
          addLogicalTypeConversions(data, f.schema(), updatedSeenSchemas)
        }
      case Schema.Type.MAP =>
        addLogicalTypeConversions(data, schema.getValueType, seenSchemas)
      case Schema.Type.ARRAY =>
        addLogicalTypeConversions(data, schema.getElementType, seenSchemas)
      case Schema.Type.UNION =>
        schema.getTypes.asScala.foreach { t =>
          addLogicalTypeConversions(data, t, seenSchemas)
        }
      case _ =>
    }
  }

}

/**
 * Compatibility with avro runtime versions newer than the one scio compiles against. All avro APIs
 * used here are accessed by reflection, so this is a no-op on versions that don't have them.
 */
private[scio] object AvroCompat {

  val FastReaderProperty = "org.apache.avro.fastread"

  private lazy val setFastReaderEnabled: Option[Method] =
    Try(classOf[GenericData].getMethod("setFastReaderEnabled", classOf[Boolean])).toOption

  /**
   * Avro 1.10+ FastReaderBuilder (enabled by default since 1.12) doesn't call
   * `GenericDatumReader.findStringClass`, so [[org.apache.avro.util.Utf8]] leaks into
   * [[CharSequence]] fields despite scio's override. Setting `org.apache.avro.fastread=true`
   * explicitly keeps the fast reader.
   */
  def disableFastReader(data: GenericData): Unit =
    if (!"true".equalsIgnoreCase(System.getProperty(FastReaderProperty))) {
      setFastReaderEnabled.foreach(_.invoke(data, java.lang.Boolean.FALSE))
    }

  private val JavaClassProps = Seq("java-class", "java-key-class", "java-element-class")

  private val trustedClassNames: java.util.Set[String] = ConcurrentHashMap.newKeySet[String]()

  /**
   * Avro 1.12.1+ validates every class it loads by name against `ClassSecurityValidator`, and
   * 1.12.2 rejects generated specific records too unless their package is listed in
   * `org.apache.avro.SERIALIZABLE_PACKAGES`. Extends the global validator to trust the classes scio
   * reads; any other class is checked by the validator in place before.
   */
  private lazy val validatorInstalled: Boolean = Try {
    val validator = Class.forName("org.apache.avro.util.ClassSecurityValidator")
    val predicate = Class.forName("org.apache.avro.util.ClassSecurityValidator$ClassSecurityPredicate")
    val previous = validator.getMethod("getGlobal").invoke(null)
    val handler = new InvocationHandler {
      override def invoke(proxy: Any, method: Method, args: Array[AnyRef]): AnyRef =
        method.getName match {
          case "isTrusted" if trustedClassNames.contains(args(0).asInstanceOf[Class[_]].getName) =>
            java.lang.Boolean.TRUE
          case "hashCode" if args == null => Int.box(System.identityHashCode(proxy))
          case "equals"                   => Boolean.box(proxy.asInstanceOf[AnyRef] eq args(0))
          case "toString" if args == null => s"ScioTrustedClasses($previous)"
          case _ =>
            try method.invoke(previous, args: _*)
            catch { case e: InvocationTargetException => throw e.getCause }
        }
    }
    val proxy = Proxy.newProxyInstance(predicate.getClassLoader, Array(predicate), handler)
    validator.getMethod("setGlobal", predicate).invoke(null, proxy)
    true
  }.getOrElse(false)

  /**
   * Trust `recordType` and the classes referenced by `schema`, its reader schema. The reader schema
   * comes from the compiled record class, unlike writer schemas which come from the data.
   */
  def trustClasses(recordType: Class[_], schema: Schema): Unit =
    if (validatorInstalled) {
      trustedClassNames.add(recordType.getName)
      collectClassNames(schema, Set.empty).foreach(trustedClassNames.add)
    }

  private def collectClassNames(schema: Schema, seen: Set[String]): Set[String] = {
    val props = JavaClassProps.flatMap(p => Option(schema.getProp(p))).toSet
    schema.getType match {
      case Schema.Type.RECORD if !seen.contains(schema.getFullName) =>
        val name = SpecificData.getClassName(schema)
        schema.getFields.asScala.foldLeft(seen + schema.getFullName + name ++ props) { (acc, f) =>
          acc ++ collectClassNames(f.schema(), acc)
        }
      case Schema.Type.RECORD                       => seen ++ props
      case Schema.Type.ENUM | Schema.Type.FIXED     => seen + SpecificData.getClassName(schema) ++ props
      case Schema.Type.MAP                          => collectClassNames(schema.getValueType, seen ++ props)
      case Schema.Type.ARRAY                        => collectClassNames(schema.getElementType, seen ++ props)
      case Schema.Type.UNION =>
        schema.getTypes.asScala.foldLeft(seen)((acc, t) => acc ++ collectClassNames(t, acc))
      case _ => seen ++ props
    }
  }
}
