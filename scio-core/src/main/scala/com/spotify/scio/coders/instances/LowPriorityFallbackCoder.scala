package com.spotify.scio.coders.instances

import com.spotify.scio.coders.{Coder, CoderMacros, LowPriorityCoderDerivation}

trait LowPriorityFallbackCoder extends LowPriorityCoderDerivation {
  import language.experimental.macros
  def fallback[T](implicit lp: shapeless.LowPriority): Coder[T] =
    macro CoderMacros.issueFallbackWarning[T]
}
