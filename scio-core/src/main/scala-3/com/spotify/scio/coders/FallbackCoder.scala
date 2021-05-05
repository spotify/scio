package com.spotify.scio.coders

import scala.reflect.ClassTag
import com.spotify.scio.coders.macros.FallbackCoderMacros

trait FallbackCoder {
  inline def fallback[T]: Coder[T] =
    ${ FallbackCoderMacros.issueFallbackWarning[T] }
}
