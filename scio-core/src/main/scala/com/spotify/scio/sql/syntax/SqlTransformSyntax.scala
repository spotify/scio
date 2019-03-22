package com.spotify.scio.sql.syntax
import com.spotify.scio.sql.{UdafFromCombineFn, Udf, UdfFromClass, UdfFromSerializableFn}
import org.apache.beam.sdk.extensions.sql.SqlTransform

import scala.language.implicitConversions

final class SqlTransformOps(private val t: SqlTransform) extends AnyVal {

  def registerUdf(udfs: Udf*): SqlTransform =
    udfs.foldLeft(t) {
      case (st, x: UdfFromClass[_]) =>
        st.registerUdf(x.fnName, x.clazz)
      case (st, x: UdfFromSerializableFn[_, _]) =>
        st.registerUdf(x.fnName, x.fn)
      case (st, x: UdafFromCombineFn[_, _, _]) =>
        st.registerUdaf(x.fnName, x.fn)
    }
}

trait SqlTransformSyntax {
  implicit def sqlSqlTransformOps(t: SqlTransform): SqlTransformOps = new SqlTransformOps(t)
}
