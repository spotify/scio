package fix.common

import scalafix.v1._
import scala.meta._

object Common {

  /** @return true if `sym` is a class whose parents include a type matching `parentMatcher` */
  def hasParentClass(sym: Symbol, parentMatcher: SymbolMatcher)(implicit
    sd: SemanticDocument
  ): Boolean = {
    sym.info.get.signature match {
      case ClassSignature(_, parents, _, _) =>
        parents
          .collect {
            case TypeRef(_, p, _) if parentMatcher.matches(p) => true
          }
          .foldLeft(false)(_ || _)
      case _ => false
    }
  }

  /**
   * Match a variable declaration, e.g. with `t @ q"..$mods val ..$patsnel: $tpeopt = $expr"` and
   * pass `t` to this function.
   *
   * @return
   *   The type parameters to the type of `t` if it matches `matcher`
   */
  def findMatchingValTypeParams(
    t: Tree,
    matcher: SymbolMatcher
  )(implicit doc: SemanticDocument): List[Symbol] = {
    // get the type of the declaration ...
    t.symbol.info.get.signature match {
      // ... and if it matches, collect all of its type parameters
      case MethodSignature(_, _, TypeRef(_, symbol, typeArgs)) if matcher.matches(symbol) =>
        typeArgs.collect { case TypeRef(_, param, _) => param }
      case _ => List.empty
    }
  }

  /**
   * Match a type application with q"$expr[..$tpesnel]".
   *
   * Given some function call with a context bound e.g.
   *   - `func[T : ContextBoundType]` or
   *   - `func[T](cbt: ContextBoundType[T])` or
   *   - `func[T](..)(implicit cbt:ContextBoundType[T])` or
   *   - `Class[T : ContextBoundType]`
   *
   * @return
   *   The symbols in `tpesnel` which are a type param to the context bound type
   */
  def findBoundedTypes(
    expr: Term,
    tpesnel: Seq[Type],
    contextBoundMatcher: SymbolMatcher
  )(implicit
    doc: SemanticDocument
  ): List[Symbol] = {
    expr.symbol.info.get.signature match {
      case MethodSignature(typeParams, parameterLists, _) =>
        // find only the types for which the context bound type is required
        val contextBoundTypeParameters = parameterLists.flatten
          .map(_.signature)
          .collect {
            case ValueSignature(TypeRef(_, symbol, args)) if contextBoundMatcher.matches(symbol) =>
              args.collect { case TypeRef(_, schemaSymbol, _) => schemaSymbol }
          }
          .flatten

        // join the actual params with the positional type params
        // and filter for the ones for which the context bound is required
        tpesnel
          .zip(typeParams)
          .collect {
            case (tpe, tParam) if contextBoundTypeParameters.contains(tParam.symbol) =>
              tpe.symbol
          }
          .toList
      case _ => List.empty
    }
  }
}
