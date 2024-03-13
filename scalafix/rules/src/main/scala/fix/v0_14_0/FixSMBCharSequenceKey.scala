package fix.v0_14_0

import fix.v0_14_0.FixSMBCharSequenceKey.{CharSequenceClassMatcher, SmbReadMatchers}
import scalafix.v1._

import scala.meta._

object FixSMBCharSequenceKey {
  val SmbReadMatchers = SymbolMatcher.normalized(
    "com/spotify/scio/smb/syntax/SortedBucketScioContext#sortMergeJoin().",
    "com/spotify/scio/smb/syntax/SortedBucketScioContext#sortMergeCoGroup().",
    "com/spotify/scio/smb/syntax/SortedBucketScioContext#sortMergeGroupByKey().",
    "com/spotify/scio/smb/syntax/SortedBucketScioContext#sortMergeTransform().",
    "com/spotify/scio/smb/util/SMBMultiJoin#sortMergeCoGroup().",
    "com/spotify/scio/smb/util/SMBMultiJoin#sortMergeTransform()."
  )

  val CharSequenceClassMatcher: SymbolMatcher = SymbolMatcher.normalized(
    "java/lang/CharSequence"
  )
}

class FixSMBCharSequenceKey extends SemanticRule("FixSMBCharSequenceKey") {

  override def fix(implicit doc: SemanticDocument): Patch = {
    doc.tree.collect {
      case t @ q"$fn(..$args)" if SmbReadMatchers.matches(fn) =>
        val argsFiltered = args.map {
          case q"classOf[$tpe]" if CharSequenceClassMatcher.matches(tpe) =>
            q"classOf[String]"
          case a => a
        }
        Patch.replaceTree(t, q"$fn(..$argsFiltered)".syntax)
    }.asPatch
  }
}
