package com.spotify.scio.coders

trait FallbackCoder {
  // TODO: scala3 - Make that the fallback implicit
  def fallback[T](implicit lp: shapeless.LowPriority): Coder[T] =
    macro CoderMacros.issueFallbackWarning[T]
}
