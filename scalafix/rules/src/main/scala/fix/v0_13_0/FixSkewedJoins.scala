package fix.v0_13_0

import scalafix.v1.{MethodSignature, _}

import scala.meta._
import scala.meta.contrib._

object FixSkewedJoins {

  private val PairSkewedSCollectionFunctions =
    "com/spotify/scio/values/PairSkewedSCollectionFunctions"

  val SkewedJoins: SymbolMatcher =
    SymbolMatcher.normalized(PairSkewedSCollectionFunctions + "#skewedJoin") +
      SymbolMatcher.normalized(PairSkewedSCollectionFunctions + "#skewedLeftOuterJoin") +
      SymbolMatcher.normalized(PairSkewedSCollectionFunctions + "#skewedFullOuterJoin")

  val HotKeyMethodImport = importer"com.spotify.scio.values.HotKeyMethod"

  val ParamRhs = Term.Name("rhs")
  val ParamHotKeyThreshold = Term.Name("hotKeyThreshold")
  val ParamEps = Term.Name("eps")
  val ParamSeed = Term.Name("seed")
  val ParamDelta = Term.Name("delta")
  val ParamSampleFraction = Term.Name("sampleFraction")
  val ParamWithReplacement = Term.Name("withReplacement")

  val OldParameters = List(
    ParamRhs.value,
    ParamHotKeyThreshold.value,
    ParamEps.value,
    ParamSeed.value,
    ParamDelta.value,
    ParamSampleFraction.value,
    ParamWithReplacement.value,
    "hasher"
  )

}

class FixSkewedJoins extends SemanticRule("FixSkewedJoins") {

  import FixSkewedJoins._

  private def isOldSkewedJoinApi(fn: Term)(implicit doc: SemanticDocument): Boolean = {
    val symbol = fn.symbol
    SkewedJoins.matches(symbol) && (symbol.info.get.signature match {
      case MethodSignature(_, parameterLists, _) =>
        parameterLists.flatten.map(_.symbol.displayName) == OldParameters
      case _ => false
    })
  }

  private def findParam(param: Term.Name, pos: Int)(args: List[Term]): Option[Term] = {
    args
      .collectFirst {
        case q"$name = $value" if name.isEqual(param) => value
      }
      .orElse {
        args.takeWhile(!_.isInstanceOf[Term.Assign]).lift(pos)
      }
  }

  override def fix(implicit doc: SemanticDocument): Patch = {
    doc.tree.collect {
      case t @ q"$fn(..$params)" if isOldSkewedJoinApi(fn) =>
        val rhs = findParam(ParamRhs, 0)(params).map(p => q"rhs = $p")
        val hotKeyMethod = findParam(ParamHotKeyThreshold, 1)(params).map(p =>
          q"hotKeyMethod = HotKeyMethod.Threshold($p)"
        )
        val cmsEps = findParam(ParamEps, 2)(params).map(p => q"cmsEps = $p")
        val cmsDelta = findParam(ParamDelta, 4)(params).map(p => q"cmsDelta = $p")
        val cmsSeed = findParam(ParamSeed, 3)(params).map(p => q"cmsSeed = $p")
        val sampleFraction =
          findParam(ParamSampleFraction, 5)(params).map(p => q"sampleFraction = $p")
        val sampleWithReplacement =
          findParam(ParamWithReplacement, 6)(params).map(p => q"sampleWithReplacement = $p")

        val updated =
          (rhs ++ hotKeyMethod ++ cmsEps ++ cmsDelta ++ cmsSeed ++ sampleFraction ++ sampleWithReplacement).toList
        Patch.addGlobalImport(HotKeyMethodImport) + Patch.replaceTree(t, q"$fn(..$updated)".syntax)
    }.asPatch
  }
}
