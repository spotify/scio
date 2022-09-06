package fix
package v0_12_0

import scalafix.v1._
import scala.meta._

class FixPubsubSpecializations extends SemanticRule("FixPubsubSpecializations") {
  private val pubSubIOPath = "com/spotify/scio/pubsub/PubsubIO."
  private val scioContextPath = "com/spotify/scio/ScioContext."

  override def fix(implicit doc: SemanticDocument): Patch = {
    doc.tree.collect {
      case a @ Term.Apply(fun, args)
          if fun.symbol.normalized.toString.startsWith("com.spotify.scio.pubsub.PubsubIO.") =>
        fun match {
          // PubsubIO[T < SpecificRecordBase](params)
          case Term.ApplyType(qual, types @ List(Type.Name(_)))
              if isSubOfType(qual.symbol, pubSubIOPath) =>

            methodCallForIOConfig(types.head)
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
      case a @ Term.Apply(fun, args)
          if fun.symbol.normalized.toString.startsWith(
            "com.spotify.scio.pubsub.syntax.SCollectionSyntax.SCollectionPubsubOps."
          ) => {
        fun match {
          //scoll.saveAsPubsub("topic")
          case Term.Select(qual, Term.Name(methodName))
            if (methodName.startsWith("saveAsPubsub"))=>
            val (methodArgs, writeParams) = splitWriteParams(args)
            Patch.replaceTree(a, s"$qual.write(PubsubIO.string(${methodArgs.mkString(", ")}))(PubsubIO.WriteParam(${writeParams.mkString(", ")}))")

          //scoll.saveAsPubsubWithAttributes("topic")
          case Term.ApplyType(qual, types @ List(Type.Name(_)))
            if (qual.symbol.toString.contains("saveAsPubsubWithAttributes")) =>
            val (methodArgs, writeParams) = splitWriteParams(args)
            val scoll = qual.toString.split("\\.").head
            methodCallForIOConfig(types.head)
              .map(c => Patch.replaceTree(a, s"$scoll.write(PubsubIO.$c(${methodArgs.mkString(", ")}))(PubsubIO.WriteParam(${writeParams.mkString(", ")}))"))
              .getOrElse(
                Patch.empty
              )

          case _ =>
            Patch.empty
        }
      }

      case a @ Term.Apply(fun, args)
          if fun.symbol.normalized.toString.startsWith(
            "com.spotify.scio.pubsub.syntax.ScioContextSyntax.ScioContextOps."
          ) =>
        {
          val readparamsTopic = "(PubsubIO.ReadParam(PubsubIO.Topic))"
          val readParamsSubs = "(PubsubIO.ReadParam(PubsubIO.Subscription))"
          fun match {
            // sc.pubsubTopic[String](params)
            case Term.ApplyType(
                  Term.Select(Term.Name(qual), Term.Name("pubsubTopic")),
                  methodType
                ) =>
              methodCallForIOConfig(methodType.head).map(c =>
                Patch.replaceTree(
                  a,
                  s"$qual.read(PubsubIO.$c(${args.mkString(", ")}))$readparamsTopic"
                )
              )
            // sc.pubsubSubscription[String](params)
            case Term.ApplyType(
                  Term.Select(Term.Name(qual), Term.Name("pubsubSubscription")),
                  methodType
                ) =>
              methodCallForIOConfig(methodType.head).map(c =>
                Patch.replaceTree(
                  a,
                  s"$qual.read(PubsubIO.$c(${args.mkString(", ")}))$readParamsSubs"
                )
              )
            // sc.pubsubTopicWithAttributes[String](params)
            case Term.ApplyType(
                  Term.Select(Term.Name(qual), Term.Name("pubsubTopicWithAttributes")),
                  methodType
                ) =>
              methodCallForIOConfig(methodType.head, true).map(c =>
                Patch.replaceTree(
                  a,
                  s"$qual.read(PubsubIO.$c(${args.mkString(", ")}))$readparamsTopic"
                )
              )
            // sc.pubsubSubscriptionWithAttributes[String](params)
            case Term.ApplyType(
                  Term.Select(Term.Name(qual), Term.Name("pubsubSubscriptionWithAttributes")),
                  methodType
                ) =>
              methodCallForIOConfig(methodType.head, true).map(c =>
                Patch.replaceTree(
                  a,
                  s"$qual.read(PubsubIO.$c(${args.mkString(", ")}))$readParamsSubs"
                )
              )
            case _ =>
              None
          }
        }.getOrElse(Patch.empty)
      case _ =>
        Patch.empty
    }.asPatch
  }

  private def splitWriteParams(args: List[Term]): (List[String], List[String]) =
    args.zipWithIndex.foldLeft((List[String](), List[String]())) {
      case ((ma, wp), (p, i)) =>
        if (i == 0) {
          (List(p.toString), List())
        } else if (
          p.toString.contains("=") && (
            p.toString.startsWith("topic") ||
              p.toString.startsWith("idAttribute") ||
              p.toString.startsWith("timestampAttribute")
            )
        ) {
          (ma :+ p.toString, wp)
        } else if (p.toString.contains("=") && p.toString.startsWith("maxBatch")) {
          (ma, wp :+ p.toString)
        } else if (i > 2) {
          (ma, wp :+ p.toString)
        } else {
          (ma :+ p.toString, wp)
        }
    }

  private def methodCallForIOConfig(termType: scala.meta.Type, withAtt: Boolean = false)(implicit
    doc: SemanticDocument
  ): Option[String] =
    if (isSubOfType(termType.symbol, "org/apache/avro/specific/SpecificRecordBase#")) {
      if (withAtt) Some(s"withAttributes[${termType}]") else Some(s"avro[${termType}]")
    } else if (isSubOfType(termType.symbol, "com/google/protobuf/Message#")) {
      if (withAtt) Some(s"withAttributes[${termType}]") else Some(s"proto[${termType}]")
    } else if (isSubOfType(termType.symbol, "org/apache/beam/sdk/io/gcp/pubsub/PubsubMessage#")) {
      if (withAtt) Some(s"withAttributes[${termType}]") else Some(s"pubsub[${termType}]")
    } else if (isSubOfType(termType.symbol, "java/lang/String#")) {
      if (withAtt) Some(s"withAttributes[${termType}]") else Some("string")
    } else {
      None
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
