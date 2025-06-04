package fix.v0_12_0

import scalafix.v1._

import scala.annotation.tailrec
import scala.meta._
import scala.meta.contrib._

object FixPubsubSpecializations {
  val PubSubIO: SymbolMatcher =
    SymbolMatcher.normalized("com/spotify/scio/pubsub/PubsubIO")
  val SCtx: SymbolMatcher =
    SymbolMatcher.normalized("com/spotify/scio/ScioContext")
  val SCtxOubSubOps: SymbolMatcher = SCtx +
    SymbolMatcher.normalized("com/spotify/scio/pubsub/syntax/ScioContextSyntax#ScioContextOps")
  val Scoll: SymbolMatcher =
    SymbolMatcher.normalized("com/spotify/scio/values/SCollection")
  val SCollPubsubOps: SymbolMatcher = Scoll +
    SymbolMatcher.normalized(
      "com/spotify/scio/pubsub/syntax/SCollectionSyntax#SCollectionPubsubOps"
    )

  val AvroSpecificRecord: SymbolMatcher =
    SymbolMatcher.normalized("org/apache/avro/specific/SpecificRecordBase")
  val ProtoMessage: SymbolMatcher =
    SymbolMatcher.normalized("com/google/protobuf/Message")
  val PubSubMessage: SymbolMatcher =
    SymbolMatcher.normalized("org/apache/beam/sdk/io/gcp/pubsub/PubsubMessage")
  val JavaString: SymbolMatcher =
    SymbolMatcher.normalized("java/lang/String")

  val PubSubIOFns: Map[String, String] = Map(
    "readAvro" -> "avro",
    "readProto" -> "proto",
    "readPubsub" -> "pubsub",
    "readCoder" -> "coder"
  )

  val ParamSub = Term.Name("sub")
  val ParamTopic = Term.Name("topic")
  val ParamIdAttribute = Term.Name("idAttribute")
  val ParamTimestampAttribute = Term.Name("timestampAttribute")
  val ParamMaxBatchSize = Term.Name("maxBatchSize")
  val ParamMaxBatchBytesSize = Term.Name("maxBatchBytesSize")

  val ReadParamTopic = q"PubsubIO.ReadParam(PubsubIO.Topic)"
  val ReadParamSub = q"PubsubIO.ReadParam(PubsubIO.Subscription)"

  object SCollectionType {

    // extracts the type parameter symbol of an SCollection SemanticType
    def unapply(semanticType: SemanticType): Option[Symbol] = semanticType match {
      case TypeRef(_, _, TypeRef(_, t, _) :: _) => Some(t)
      case _                                    => None
    }
  }

}

class FixPubsubSpecializations extends SemanticRule("FixPubsubSpecializations") {

  import FixPubsubSpecializations._

  def isSubtypeOf(
    expected: SymbolMatcher
  )(symbol: Symbol)(implicit doc: SemanticDocument): Boolean = {
    @tailrec def go(isSub: Boolean, sym: List[Symbol]): Boolean = {
      (isSub, sym) match {
        case (true, _)        => true
        case (false, Nil)     => false
        case (false, s :: ss) =>
          val parents = s.info.map(_.signature) match {
            case Some(ClassSignature(_, parents, _, _)) =>
              parents.collect { case TypeRef(_, p, _) => p }
            case Some(TypeSignature(_, TypeRef(_, lowerBound, _), TypeRef(_, upperBound, _))) =>
              lowerBound :: upperBound :: Nil
            case _ =>
              Nil
          }
          go(expected.matches(s), parents ++ ss)
      }
    }

    go(false, List(symbol))
  }

  private def scollType(term: Term)(implicit doc: SemanticDocument): Option[Symbol] = {
    term.symbol.info.map(_.signature) match {
      case Some(MethodSignature(_, _, SCollectionType(t)))            => Some(t)
      case Some(ValueSignature(AnnotatedType(_, SCollectionType(t)))) => Some(t)
      case Some(ValueSignature(SCollectionType(t)))                   => Some(t)
      case _                                                          => None
    }
  }

  private def expectedType(
    expected: SymbolMatcher
  )(qual: Term)(implicit doc: SemanticDocument): Boolean =
    qual.symbol.info.get.signature match {
      case MethodSignature(_, _, TypeRef(_, typ, _)) =>
        expected.matches(typ)
      case ValueSignature(AnnotatedType(_, TypeRef(_, typ, _))) =>
        expected.matches(typ)
      case ValueSignature(TypeRef(_, typ, _)) =>
        expected.matches(typ)
      case _ =>
        false
    }

  private def findParam(param: Term.Name, pos: Int)(args: List[Term]): Option[Term] = {
    args
      .collectFirst {
        case p @ q"$name = $_" if name.isEqual(param) => p
      }
      .orElse {
        args.takeWhile(!_.isInstanceOf[Term.Assign]).lift(pos)
      }
  }

  private def splitArgs(args: List[Term]): (List[Term], List[Term]) = {
    val topic = findParam(ParamTopic, 0)(args)
    val idAttribute = findParam(ParamIdAttribute, 1)(args)
    val timestampAttribute = findParam(ParamTimestampAttribute, 2)(args)
    val maxBatchSize = findParam(ParamMaxBatchSize, 3)(args)
    val maxBatchBytesSize = findParam(ParamMaxBatchBytesSize, 4)(args)
    val ioArgs = (topic ++ idAttribute ++ timestampAttribute).toList
    val paramsArgs = (maxBatchSize ++ maxBatchBytesSize).toList
    (ioArgs, paramsArgs)
  }

  private def buildPubsubIO(tp: Type, sym: Symbol, args: List[Term])(implicit
    doc: SemanticDocument
  ): Term = {
    if (isSubtypeOf(AvroSpecificRecord)(sym)) q"PubsubIO.avro[$tp](..$args)"
    else if (isSubtypeOf(ProtoMessage)(sym)) q"PubsubIO.proto[$tp](..$args)"
    else if (isSubtypeOf(PubSubMessage)(sym)) q"PubsubIO.pubsub[$tp](..$args)"
    else if (isSubtypeOf(JavaString)(sym)) q"PubsubIO.string(..$args)"
    else throw new Exception(s"Unsupported type in pubsub '$tp'")
  }

  override def fix(implicit doc: SemanticDocument): Patch = {
    doc.tree.collect {
      case t @ q"$fn[$tp](..$args)" if PubSubIO.matches(fn.symbol) =>
        // PubsubIO[T](args)
        val io = buildPubsubIO(tp, tp.symbol, args)
        Patch.replaceTree(t, io.syntax)
      case t @ q"$qual.$fn[$tp](..$args)"
          // PubsubIO.$fn[T](args)
          if PubSubIO.matches(qual.symbol) && PubSubIOFns.contains(fn.value) =>
        val renamedFn = Term.Name(PubSubIOFns(fn.value))
        Patch.replaceTree(t, q"$qual.$renamedFn[$tp](..$args)".syntax)
      case t @ q"$qual.readString(..$args)" if PubSubIO.matches(qual.symbol) =>
        // PubsubIO.readString(args)
        Patch.replaceTree(t, q"$qual.string(..$args)".syntax)
      case t @ q"$qual.saveAsPubsub(..$args)" if expectedType(SCollPubsubOps)(qual) =>
        scollType(qual) match {
          case Some(tpSym) =>
            val (ioArgs, paramArgs) = splitArgs(args)
            val tp = Type.Name(tpSym.displayName) // may require import, but best effort
            val io = buildPubsubIO(tp, tpSym, ioArgs)
            val params = q"PubsubIO.WriteParam(..$paramArgs)"
            Patch.replaceTree(t, q"$qual.write($io)($params)".syntax)
          case None =>
            // We did not managed to extract the type from the SCollection
            Patch.empty
        }
      case t @ q"$qual.saveAsPubsubWithAttributes[$tp](..$args)"
          if expectedType(SCollPubsubOps)(qual) =>
        val (ioArgs, paramArgs) = splitArgs(args)
        val io = q"PubsubIO.withAttributes[$tp](..$ioArgs)"
        val params = q"PubsubIO.WriteParam(..$paramArgs)"
        Patch.replaceTree(t, q"$qual.write($io)($params)".syntax)
      case t @ q"$qual.pubsubSubscription[$tp](..$args)" if expectedType(SCtxOubSubOps)(qual) =>
        val io = buildPubsubIO(tp, tp.symbol, args)
        Patch.replaceTree(t, q"$qual.read($io)($ReadParamSub)".syntax)
      case t @ q"$qual.pubsubSubscriptionWithAttributes[$tp](..$args)"
          if expectedType(SCtxOubSubOps)(qual) =>
        val io = q"PubsubIO.withAttributes[$tp](..$args)"
        Patch.replaceTree(t, q"$qual.read($io)($ReadParamSub)".syntax)
      case t @ q"$qual.pubsubTopic[$tp](..$args)" if expectedType(SCtxOubSubOps)(qual) =>
        val io = buildPubsubIO(tp, tp.symbol, args)
        Patch.replaceTree(t, q"$qual.read($io)($ReadParamTopic)".syntax)
      case t @ q"$qual.pubsubTopicWithAttributes[$tp](..$args)"
          if expectedType(SCtxOubSubOps)(qual) =>
        val io = q"PubsubIO.withAttributes[$tp](..$args)"
        Patch.replaceTree(t, q"$qual.read($io)($ReadParamTopic)".syntax)
    }.asPatch
  }
}
