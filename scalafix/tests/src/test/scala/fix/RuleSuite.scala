package fix

import org.scalatest.FunSuiteLike
import scalafix.testkit.AbstractSemanticRuleSuite

class RuleSuite extends AbstractSemanticRuleSuite with FunSuiteLike {
  runAllTests()
}
