package com.spotify.scio.schemas

import scala.reflect.ClassTag
import org.apache.beam.sdk.values.TupleTag

private[scio] trait SchemaMacroHelpers {
  import scala.reflect.macros._
  import com.spotify.scio.{FeatureFlag, MacroSettings}

  val ctx: blackbox.Context
  import ctx.universe._

  val cacheImplicitSchemas: FeatureFlag = MacroSettings.cacheImplicitSchemas(ctx)

  def untyped[A: ctx.WeakTypeTag](expr: ctx.Expr[Schema[A]]): ctx.Expr[Schema[A]] =
    ctx.Expr[Schema[A]](ctx.untypecheck(expr.tree.duplicate))

  def inferImplicitSchema[A: ctx.WeakTypeTag]: ctx.Expr[Schema[A]] =
    inferImplicitSchema(weakTypeOf[A]).asInstanceOf[ctx.Expr[Schema[A]]]

  def inferImplicitSchema(t: ctx.Type): ctx.Expr[Schema[_]] = {
    val tpe =
      cacheImplicitSchemas match {
        case FeatureFlag.Enable =>
          tq"_root_.shapeless.Cached[_root_.com.spotify.scio.schemas.Schema[$t]]"
        case _ =>
          tq"_root_.com.spotify.scio.schemas.Schema[$t]"
      }

    val tp = ctx.typecheck(tpe, ctx.TYPEmode).tpe
    val typedTree = ctx.inferImplicitValue(tp, silent = false)
    val untypedTree = ctx.untypecheck(typedTree.duplicate)

    cacheImplicitSchemas match {
      case FeatureFlag.Enable =>
        ctx.Expr[Schema[_]](q"$untypedTree.value")
      case _ =>
        ctx.Expr[Schema[_]](untypedTree)
    }
  }

  def inferClassTag(t: ctx.Type): ctx.Expr[ClassTag[_]] =
    ctx.Expr[ClassTag[_]](q"implicitly[_root_.scala.reflect.ClassTag[$t]]")

  implicit def liftTupleTag[A: ctx.WeakTypeTag]: Liftable[TupleTag[A]] = Liftable[TupleTag[A]] {
    x => q"new _root_.org.apache.beam.sdk.values.TupleTag[${weakTypeOf[A]}](${x.getId()})"
  }
}
