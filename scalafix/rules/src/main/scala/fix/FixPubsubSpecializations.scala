package fix
package v0_12_0

import scalafix.v1._
import scala.meta._

class FixPubsubSpecializations extends SemanticRule("FixPubsubSpecializations") {
  private val pubSubIOPath = "com/spotify/scio/pubsub/PubsubIO."

  override def fix(implicit doc: SemanticDocument): Patch = {
    doc.tree.collect {
      case a @ Term.Apply(fun, args)
          if fun.symbol.normalized.toString.startsWith("com.spotify.scio.pubsub.PubsubIO.") =>
        fun match {
          // PubsubIO[T < SpecificRecordBase](params)
          case Term.ApplyType(qual, types @ List(Type.Name(_)))
              if isSubOfType(qual.symbol, pubSubIOPath) =>
            expectedType(qual, "")
            val methodCall =
              if (isSubOfType(types.head.symbol, "org/apache/avro/specific/SpecificRecordBase#")) {
                Some(s"avro[${types.head}]")
              } else if (isSubOfType(types.head.symbol, "com/google/protobuf/Message#")) {
                Some(s"proto[${types.head}]")
              } else if (
                isSubOfType(types.head.symbol, "org/apache/beam/sdk/io/gcp/pubsub/PubsubMessage#")
              ) {
                Some(s"pubsub[${types.head}]")
              } else if (isSubOfType(types.head.symbol, "java/lang/String#")) {
                Some("string")
              } else {
                None
              }

            methodCall
              .map(c => Patch.replaceTree(a, s"$qual.$c(${args.mkString(", ")})"))
              .getOrElse(
                Patch.empty
              )

          // PubsubIO.readAvro(params)
          case Term.ApplyType(Term.Select(qual, Term.Name(methodName)), methodType)
              if isSubOfType(qual.symbol, pubSubIOPath) =>
            (
              methodName match {
                case "readAvro"   => Some("avro")
                case "readProto"  => Some("proto")
                case "readPubsub" => Some("pubsub")
                case "readCoder"  => Some("coder")
                case _            => None
              }
            ).map(name =>
              Patch.replaceTree(a, s"$qual.$name[${methodType.head}](${args.mkString(", ")})")
            ).getOrElse(Patch.empty)

          // PubsubIO.readString(params)
          case Term.Select(qual, Term.Name("readString"))
              if isSubOfType(qual.symbol, pubSubIOPath) =>
            Patch.replaceTree(a, s"$qual.string(${args.mkString(", ")})")
          case _ =>
            Patch.empty
        }

      case _ =>
        Patch.empty
    }.asPatch
  }

  private def expectedType(qual: Term, typStr: String)(implicit doc: SemanticDocument): Boolean = {
    qual.symbol.info.get.signature match {
      case MethodSignature(_, _, TypeRef(_, typ, _)) =>
        SymbolMatcher.exact(typStr).matches(typ)
      case ValueSignature(AnnotatedType(_, TypeRef(_, typ, _))) =>
        SymbolMatcher.exact(typStr).matches(typ)
      case ValueSignature(TypeRef(_, typ, _)) =>
        SymbolMatcher.exact(typStr).matches(typ)
      case _ =>
        false
    }
  }

  def isSubOfType(symbol: Symbol, typeStr: String)(implicit doc: SemanticDocument): Boolean =
    getParentSymbols(symbol).map(_.toString).contains(typeStr)

  def getParentSymbols(symbol: Symbol)(implicit doc: SemanticDocument): Set[Symbol] = {
    symbol.info match {
      case Some(info) =>
        info.signature match {
          case ClassSignature(_, parents, _, _) =>
            Set(symbol) ++ parents.collect { case TypeRef(_, symbol, _) =>
              getParentSymbols(symbol)
            }.flatten
          case TypeSignature(_, TypeRef(_, lowerBound, _), TypeRef(_, upperBound, _)) =>
            Set(symbol) ++ getParentSymbols(lowerBound) ++ getParentSymbols(upperBound)
          case _ =>
            Set()
        }
      case _ =>
        Set()
    }
  }
}
