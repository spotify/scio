package fix
package v0_12_0

import scalafix.v1._
import scala.meta._

class FixPubsubSpecializations extends SemanticRule("FixPubsubSpecializations") {
//  private val pubSubIOStr = "com/spotify/scio/pubsub/PubsubIO."
  private val pubSubIOStr = "com.spotify.scio.pubsub.PubsubIO."

  override def fix(implicit doc: SemanticDocument): Patch = {
    doc.tree.collect {
      case a @ Term.Apply(fun, args) if fun.symbol.normalized.toString.startsWith(pubSubIOStr) =>
        fun match {
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

          case Term.Select(qual, Term.Name("readString")) =>
            Patch.replaceTree(a, s"$qual.string(${args.mkString(", ")})")
          case _ =>
            Patch.empty
        }

      case _ =>
        Patch.empty
    }.asPatch
  }
}
