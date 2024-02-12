package fix.v0_14_0

import scalafix.v1._

import scala.meta._

object FixAvroCoder {
  val avroImport = importer"com.spotify.scio.avro._"
  val CoderMatcher: SymbolMatcher = SymbolMatcher.normalized(
    "com/spotify/scio/coders/Coder"
  )
  val AvroCoderPath: String = "com/spotify/scio/coders/instances/AvroCoders"
  val AvroCoderMatcher: SymbolMatcher = SymbolMatcher.normalized(
    AvroCoderPath + "#avroGenericRecordCoder",
    AvroCoderPath + "#avroSpecificRecordCoder",
    AvroCoderPath + "#avroSpecificFixedCoder"
  )
  val AvroMatcher: SymbolMatcher = SymbolMatcher.normalized(
    "org/apache/avro/specific/SpecificRecord",
    "org/apache/avro/specific/SpecificFixed",
    "org/apache/avro/generic/GenericRecord"
  )

  val JobTestBuilderMatcher: SymbolMatcher = SymbolMatcher.normalized(
    "com/spotify/scio/testing/JobTest.Builder#input().",
    "com/spotify/scio/testing/JobTest.Builder#output()."
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

  def isAvroType(sym: Symbol)(implicit sd: SemanticDocument): Boolean =
    AvroMatcher.matches(sym) || hasParentClass(sym, AvroMatcher)
}

class FixAvroCoder extends SemanticRule("FixAvroCoder") {
  import FixAvroCoder._

  override def fix(implicit doc: SemanticDocument): Patch = {
    val usesAvroCoders = doc.tree
      .collect {
        case q"$expr[..$tpesnel]" =>
          // A method call with a type parameter requires an implicit Coder[T] for our type
          findBoundedTypes(expr, tpesnel, CoderMatcher)
            .exists(isAvroType)
        case t @ q"..$mods val ..$patsnel: $tpeopt = $expr" =>
          // Coder[T] is a variable type where T is an avro type
          findMatchingValTypeParams(t, CoderMatcher)
            .exists(isAvroType)
        case q"$jobTestBuilder(..$args)" if JobTestBuilderMatcher.matches(jobTestBuilder) =>
          args.headOption match {
            case Some(q"$io[$tpe](..$args)") =>
              tpe.symbol.info.map(_.signature) match {
                case Some(ClassSignature(_, parents, _, _)) =>
                  parents.exists {
                    case TypeRef(_, s, _) if AvroMatcher.matches(s) => true
                    case _ => false
                  }
                case _ => false
              }
            case _ => false
          }
      }
      .foldLeft(false)(_ || _)
    val avroValuePatch = if (usesAvroCoders) Patch.addGlobalImport(avroImport) else Patch.empty

    val patches = doc.tree.collect {
      case importer"com.spotify.scio.coders.Coder.{..$imps}" =>
        // fix direct import from Coder
        imps.collect {
          case i @ (importee"avroGenericRecordCoder" | importee"avroSpecificRecordCoder" |
              importee"avroSpecificFixedCoder") =>
            Patch.removeImportee(i) + Patch.addGlobalImport(avroImport)
        }.asPatch
      case importer"com.spotify.scio.avro.{..$imps}" =>
        imps.map(i => Patch.removeImportee(i) + Patch.addGlobalImport(avroImport)).asPatch
      case t @ q"$obj.$fn" if AvroCoderMatcher.matches(fn.symbol) =>
        // fix direct usage of Coder.avro*
        Patch.replaceTree(t, q"$fn".syntax) + Patch.addGlobalImport(avroImport)
    }.asPatch

    patches + avroValuePatch
  }
}
