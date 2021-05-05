package com.spotify.scio.sql

import com.spotify.scio.annotations.experimental

trait TypedSQLInterpolator {
  // TODO: scala3 support
  @experimental
  def tsql(ps: Any*): SQLBuilder = ???
}
