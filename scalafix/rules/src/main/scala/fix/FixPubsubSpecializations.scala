package fix
package v0_12_0

import scalafix.v1._
import scala.meta._

class FixPubsubSpecializations extends SemanticRule("FixPubsubSpecializations") {
  private val pubSubIOPackage = "com.spotify.scio.pubsub.PubsubIO."

  override def fix(implicit doc: SemanticDocument): Patch = {
    doc.tree.collect {
      case a @ Term.Apply(fun, args)
          if fun.symbol.normalized.toString.startsWith(pubSubIOPackage) =>
        fun match {
          // PubsubIO[T < SpecificRecordBase](params)
          case Term.ApplyType(Term.Name("PubsubIO"), types @ List(Type.Name(_))) =>
            val methodName =
              if (isSubOfType(types.head.symbol, "org/apache/avro/specific/SpecificRecordBase#")) {
                Some("avro")
              } else if (isSubOfType(types.head.symbol, "com/google/protobuf/Message#")) {
                Some("proto")
              } else if (
                isSubOfType(types.head.symbol, "org/apache/beam/sdk/io/gcp/pubsub/PubsubMessage#")
              ) {
                Some("pubsub")
              } else if (isSubOfType(types.head.symbol, "java/lang/String#")) {
                Some("string")
              } else {
                None
              }

            methodName
              .map(n => Patch.replaceTree(a, s"PubsubIO.$n[${types.head}](${args.mkString(", ")})"))
              .getOrElse(
                Patch.empty
              )

          case Term.ApplyType(Term.Select(qual, Term.Name(methodName)), methodType) =>
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
          case Term.Select(qual, Term.Name("readString")) =>
            Patch.replaceTree(a, s"$qual.string(${args.mkString(", ")})")
          case _ =>
            Patch.empty
        }

      case _ =>
        Patch.empty
    }.asPatch
  }

  def isSubOfType(symbol: Symbol, typeStr: String)(implicit doc: SemanticDocument): Boolean =
    getParentSymbols(symbol).map(_.toString).contains(typeStr)

  def getParentSymbols(symbol: Symbol)(implicit doc: SemanticDocument): Set[Symbol] =
    symbol.info.get.signature match {
      case ClassSignature(_, parents, _, _) =>
        Set(symbol) ++ parents.collect { case TypeRef(_, symbol, _) =>
          getParentSymbols(symbol)
        }.flatten
      case TypeSignature(_, TypeRef(_, lowerBound, _), TypeRef(_, upperBound, _)) =>
        Set(symbol) ++ getParentSymbols(lowerBound) ++ getParentSymbols(upperBound)
    }
}
