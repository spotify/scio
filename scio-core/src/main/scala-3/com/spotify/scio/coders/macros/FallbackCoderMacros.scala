package com.spotify.scio.coders.macros

import com.spotify.scio.coders.Coder
import scala.compiletime._
import scala.deriving._
import scala.quoted._
import scala.reflect.ClassTag

object FallbackCoderMacros {

  private[this] var verbose = true
  private[this] val reported: scala.collection.mutable.Set[(String, String)] =
    scala.collection.mutable.Set.empty

  private[this] val BlacklistedTypes = List("org.apache.beam.sdk.values.Row")

  private[this] val Warnings =
    Map(
      "org.apache.avro.generic.GenericRecord" ->
        """
          |Using a fallback coder for Avro's GenericRecord is discouraged as it is VERY inefficient.
          |It is highly recommended to define a proper Coder[GenericRecord] using:
          |
          |  Coder.avroGenericRecordCoder(schema)
        """.stripMargin
    )

  def issueFallbackWarning[T](using Quotes, Type[T]): Expr[Coder[T]] = {
    import quotes.reflect._
    // TODO: scala3 implement macro settings
    // val show = MacroSettings.showCoderFallback(c) == FeatureFlag.Enable
    val show = true

    val fullTypeColored = Type.show[T]
    val fullType = Type.show[T]
    val typeName: String = fullType.split('.').last // TODO: Type.showShort[T] ?

    val toReport = Position.ofMacroExpansion.toString -> fullType
    val alreadyReported = reported.contains(toReport)
    if (!alreadyReported) reported += toReport

     val shortMessage =
      s"""
      | Warning: No implicit Coder found for the following type:
      |
      |   >> $fullTypeColored
      |
      | using Kryo fallback instead.
      """

    val longMessage =
      shortMessage +
        s"""
        |
        |  Scio will use a fallback Kryo coder instead.
        |
        |  If a type is not supported, consider implementing your own implicit Coder for this type.
        |  It is recommended to declare this Coder in your class companion object:
        |
        |       object $typeName {
        |         import com.spotify.scio.coders.Coder
        |         import org.apache.beam.sdk.coders.AtomicCoder
        |
        |         implicit def coder$typeName: Coder[$fullType] =
        |           Coder.beam(new AtomicCoder[$fullType] {
        |             def decode(in: InputStream): $fullType = ???
        |             def encode(ts: $fullType, out: OutputStream): Unit = ???
        |           })
        |       }
        |
        |  If you do want to use a Kryo coder, be explicit about it:
        |
        |       implicit def coder$typeName: Coder[$fullType] = Coder.kryo[$fullType]
        |
        |  Additional info at:
        |   - https://spotify.github.io/scio/internals/Coders
        |
        """

    val Some(tag) = Expr.summon[ClassTag[T]]
    val fallback = '{ Coder.kryo[T]($tag) }

    (verbose, alreadyReported) match {
      case _ if BlacklistedTypes.contains(fullType) =>
        val msg =
          s"Can't use a Kryo coder for $fullType. You need to explicitly set the Coder for this type"
        report.throwError(msg)
      case _ if Warnings.contains(fullType) =>
        report.warning(Warnings(fullType))
        fallback
      case (false, false) =>
        if (show) report.warning(shortMessage.stripMargin)
        fallback
      case (true, false) =>
        if (show) report.warning(longMessage.stripMargin)
        verbose = false
        fallback
      case (_, _) =>
        fallback
    }

  }

}
