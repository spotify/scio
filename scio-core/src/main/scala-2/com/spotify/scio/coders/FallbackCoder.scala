package com.spotify.scio.coders

trait FallbackCoder {
  def fallback[T](implicit lp: shapeless.LowPriority): Coder[T] =
    macro CoderMacros.issueFallbackWarning[T]
}
