package com.spotify.scio.sql

import com.spotify.scio.annotations.experimental
import com.spotify.scio.values.SCollection
import com.spotify.scio.schemas.Schema
import org.apache.beam.sdk.values.TupleTag

import scala.reflect.macros.{blackbox, whitebox}
import com.spotify.scio.schemas.SchemaMacroHelpers

import scala.reflect._


trait TypedSQLInterpolator {
  @experimental
  def tsql(ps: Any*): SQLBuilder =
    macro SqlInterpolatorMacro.builder
}


private trait SqlInterpolatorMacroHelpers {
  val ctx: blackbox.Context
  import ctx.universe._

  def partsFromContext: List[Tree] =
    ctx.prefix.tree match {
      case Apply(_, Apply(_, xs: List[_]) :: Nil) => xs
      case tree =>
        ctx.abort(
          ctx.enclosingPosition,
          s"Implementation error. Expected tsql string interpolation, found $tree"
        )
    }

  def buildSQLString(parts: List[Tree], tags: List[String]): String = {
    val ps2 =
      parts.map {
        case Literal(Constant(s: String)) => s
        case tree =>
          ctx.abort(
            ctx.enclosingPosition,
            s"Implementation error. Expected Literal(Constant(...)), found $tree"
          )
      }

    ps2
      .zipAll(tags, "", "")
      .foldLeft("") { case (a, (x, y)) => s"$a$x $y" }
  }

  def tagFor(t: Type, lbl: String): Tree =
    q"new _root_.org.apache.beam.sdk.values.TupleTag[$t]($lbl)"
}

object SqlInterpolatorMacro {

  /** This static annotation is used to pass (static) parameters to SqlInterpolatorMacro.expand */
  final class SqlParts(parts: List[String], ps: Any*) extends scala.annotation.StaticAnnotation

  // For some reason this method needs to be a whitebox macro
  def builder(c: whitebox.Context)(ps: c.Expr[Any]*): c.Expr[SQLBuilder] = {
    val h = new { val ctx: c.type = c } with SqlInterpolatorMacroHelpers
    import h._
    import c.universe._

    val parts = partsFromContext

    val className = TypeName(c.freshName("SQLBuilder"))
    val fakeName = TypeName(c.freshName("FakeImpl"))

    // Yo Dawg i herd you like macros...
    //
    // The following tree generates an anonymous class to lazily expand tsqlImpl.
    //
    // It basically acts as curryfication of the macro,
    // where the interpolated String and it's parameters are partially applied
    // while the expected output type (and therefore the expected data schema) stays unapplied.
    // Sadly macro do not allow explicit parameter passing so the following code would be illegal
    // and the macro expansion would fail with: "term macros cannot override abstract methods"
    //   def as[B: Schema]: SCollection[B] =
    //     macro _root_.com.spotify.scio.sql.SqlInterpolatorMacro.expand[B](parts, ps)
    //
    // We workaround the limitation by using a StaticAnnotation to pass static values,
    // as described in: https://stackoverflow.com/a/25219644/2383092
    //
    // It is also illegal for a macro to override abstract methods,
    // which is why an intermediate class $fakeName is introduced.
    // Note that we HAVE TO extend SQLBuilder, otherwise, `tsqlImpl` fails to see the concrete
    // type of B, which also makes the macro expansion fails.
    val tree =
      q"""
        {
          import _root_.com.spotify.scio.values.SCollection
          import _root_.com.spotify.scio.schemas.Schema
          import _root_.scala.reflect.ClassTag

          sealed trait $fakeName  extends _root_.com.spotify.scio.sql.SQLBuilder {
            def as[B: Schema: ClassTag]: SCollection[B] = ???
          }

          final class $className extends $fakeName {
            import scala.language.experimental.macros

            @_root_.com.spotify.scio.sql.SqlInterpolatorMacro.SqlParts(List(..$parts),..$ps)
            override def as[B: Schema: ClassTag]: SCollection[B] =
              macro _root_.com.spotify.scio.sql.SqlInterpolatorMacro.expand[B]
          }
          new $className
        }
        """

    c.Expr[SQLBuilder](tree)
  }

  def expand[B: c.WeakTypeTag](
    c: blackbox.Context
  )(schB: c.Expr[Schema[B]], classTag: c.Expr[ClassTag[B]]): c.Expr[SCollection[B]] = {
    import c.universe._

    val annotationParams =
      c.macroApplication.symbol.annotations
        .filter(_.tree.tpe <:< typeOf[SqlParts])
        .flatMap(_.tree.children.tail)

    if (annotationParams.isEmpty)
      c.abort(c.enclosingPosition, "Annotation body not provided!")

    val ps: List[c.Expr[Any]] =
      annotationParams.tail.map(t => c.Expr[Any](t))

    val parts =
      annotationParams.head match {
        case Apply(TypeApply(Select(Select(_, _), TermName("apply")), _), pas) =>
          pas
        case tree =>
          c.abort(
            c.enclosingPosition,
            s"Failed to extract SQL parts. Expected List(...), found $tree"
          )
      }

    tsqlImpl[B](c)(parts, ps: _*)(classTag)
  }

  def tsqlImpl[B: c.WeakTypeTag](
    c: blackbox.Context
  )(parts: List[c.Tree], ps: c.Expr[Any]*)(
    ct: c.Expr[ClassTag[B]]
  ): c.Expr[SCollection[B]] = {
    val h = new { val ctx: c.type = c } with SqlInterpolatorMacroHelpers with SchemaMacroHelpers
    import h._
    import c.universe._

    val (ss, other) =
      ps.partition(_.actualType.typeSymbol == typeOf[SCollection[Any]].typeSymbol)

    other.headOption.foreach { t =>
      c.abort(
        c.enclosingPosition,
        s"tsql interpolation only support arguments of type SCollection. Found $t"
      )
    }

    val scs: List[(Tree, Type)] =
      ss.map { p =>
        val a = p.actualType.typeArgs.head
        (p.tree, a)
      }.toList

    val distinctSCollections =
      scs.map { case (tree, t) =>
        (tree.symbol, (tree, t))
      }.toMap

    def toSCollectionName(s: Tree) = s.symbol.name.encodedName.toString

    distinctSCollections.values.toList match {
      case list if list.size <= 10 =>
        val colls = list.map(_._1)
        val types = list.map(_._2)
        val tags = list.map(x => tagFor(x._2, toSCollectionName(x._1)))
        val sql = buildSQLString(parts, scs.map(x => toSCollectionName(x._1)))
        val implOut = inferImplicitSchema[B]
        val implIn = types.flatMap(t => Seq(inferImplicitSchema(t), inferClassTag(t)))

        val queryTree = c.parse(s"_root_.com.spotify.scio.sql.Query${types.size}")
        val q = q"$queryTree.typed[..${types :+ weakTypeOf[B]}]($sql, ..$tags)"
        c.Expr[SCollection[B]](q"""
            _root_.com.spotify.scio.sql.Sql
                .from(..$colls)(..$implIn)
                .queryAs($q)($implOut, $ct)""")
      case d =>
        val ns = d.map(_._1).mkString(", ")
        c.abort(
          c.enclosingPosition,
          s"Joins limited up to 10 SCollections, found ${d.size}: $ns"
        )
    }
  }
}

