package com.spotify.scio.schemas

import com.spotify.scio.{FeatureFlag, MacroSettings}
import org.apache.beam.sdk.values.TupleTag
import scala.reflect.ClassTag
import scala.quoted._
import scala.annotation.experimental

@experimental
private[scio] class SchemaMacroHelpers(val quotes: Quotes) {

  import quotes.reflect._

  val cacheImplicitSchemas: FeatureFlag = MacroSettings.cacheImplicitSchemas(quotes)

  // def untyped[A](expr: Expr[Schema[A]]): Expr[Schema[A]] =
  //   Expr[Schema[A]](ctx.untypecheck(expr.tree.duplicate))

  def inferImplicitSchema[T: Type](using Quotes): Expr[Schema[T]] = {
    // TODO migration: shapeless style caching not yet implemented in Scala 3 
    '{ scala.compiletime.summonInline[com.spotify.scio.schemas.Schema[T]] }
  }

  def inferClassTag[T: Type](using Quotes): Expr[ClassTag[_]] =
      '{ scala.compiletime.summonInline[_root_.scala.reflect.ClassTag[T]]}

  given liftTupleTag[A: Type](using Quotes): ToExpr[TupleTag[A]] = new ToExpr[TupleTag[A]] {
    def apply(x: TupleTag[A])(using Quotes): Expr[TupleTag[A]] = 
      '{ new org.apache.beam.sdk.values.TupleTag[A](${Expr(x.getId())}) }
  }

}
