package fix.v0_14_0

import scalafix.v1._

import scala.meta._

object FixAvroSchemasPackage {
  val avroImport = importer"com.spotify.scio.avro.schemas._"
  val fromAvroMatcher: SymbolMatcher = SymbolMatcher.normalized(
    "com/spotify/scio/schemas/instances/AvroInstances#fromAvroSchema"
  )
  val SchemaMatcher: SymbolMatcher = SymbolMatcher.normalized(
    "com/spotify/scio/schemas/Schema"
  )
  val SpecificRecordMatcher: SymbolMatcher = SymbolMatcher.normalized(
    "org/apache/avro/specific/SpecificRecord"
  )

  /** @return true if `sym` is a class whose parents include a type matching `parentMatcher` */
  def hasParentClass(sym: Symbol, parentMatcher: SymbolMatcher)(implicit
    sd: SemanticDocument
  ): Boolean = {
    sym.info.exists { i =>
      i.signature match {
        case ClassSignature(_, parents, _, _) =>
          parents
            .collect {
              case TypeRef(_, p, _) if parentMatcher.matches(p) => true
            }
            .foldLeft(false)(_ || _)
        case _ => false
      }
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
    t.symbol.info
      .map { i =>
        i.signature match {
          // ... and if it matches, collect all of its type parameters
          case MethodSignature(_, _, TypeRef(_, symbol, typeArgs)) if matcher.matches(symbol) =>
            typeArgs.collect { case TypeRef(_, param, _) => param }
          case _ => List.empty
        }
      }
      .getOrElse(List.empty)
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
    expr.symbol.info
      .map { i =>
        i.signature match {
          case MethodSignature(typeParams, parameterLists, _) =>
            // find only the types for which the context bound type is required
            val contextBoundTypeParameters = parameterLists.flatten
              .map(_.signature)
              .collect {
                case ValueSignature(TypeRef(_, symbol, args))
                    if contextBoundMatcher.matches(symbol) =>
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
      .getOrElse(List.empty)
  }

  def isSpecificRecord(sym: Symbol)(implicit sd: SemanticDocument) =
    hasParentClass(sym, SpecificRecordMatcher)

  def avroImportee(imps: Seq[Importee]): Patch = {
    imps.collect { case i @ importee"fromAvroSchema" =>
      Patch.removeImportee(i) + Patch.addGlobalImport(avroImport)
    }.asPatch
  }
}

class FixAvroSchemasPackage extends SemanticRule("FixAvroSchemasPackage") {
  import FixAvroSchemasPackage._

  override def fix(implicit doc: SemanticDocument): Patch = {
    val hasAvroSomewhere: Boolean = doc.tree
      .collect {
        case q"$expr[..$tpesnel]" =>
          // A method call with a type parameter requires an implicit Schema[T] for our type
          findBoundedTypes(expr, tpesnel, SchemaMatcher)
            .exists(isSpecificRecord)
        case t @ q"..$mods val ..$patsnel: $tpeopt = $expr" =>
          // Schema[T] is a variable type where T <: SpecificRecord
          findMatchingValTypeParams(t, SchemaMatcher)
            .exists(isSpecificRecord)
      }
      .foldLeft(false)(_ || _)
    val avroValuePatch = if (hasAvroSomewhere) Patch.addGlobalImport(avroImport) else Patch.empty

    val patches = doc.tree.collect {
      case importer"com.spotify.scio.schemas.Schema.{..$imps}" =>
        // fix direct import of fromAvroSchema
        avroImportee(imps)
      case importer"com.spotify.scio.schemas.instances.AvroInstances.{..$imps}" =>
        // fix direct import of fromAvroSchema
        avroImportee(imps)
      case t @ q"$obj.$fn" if fromAvroMatcher.matches(fn.symbol) =>
        // fix direct usage of Schema.fromAvroSchema
        Patch.replaceTree(t, q"$fn".syntax) + Patch.addGlobalImport(avroImport)
    }.asPatch

    patches + avroValuePatch
  }
}
