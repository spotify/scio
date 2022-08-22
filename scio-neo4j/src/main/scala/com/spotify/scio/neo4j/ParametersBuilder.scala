package com.spotify.scio.neo4j

trait ParametersBuilder[T] extends java.io.Serializable {
  def apply(x: T): Map[String, AnyRef]
}

object ParametersBuilder {

  def apply[T](implicit instance: ParametersBuilder[T]): ParametersBuilder[T] = instance
  def instance[T](f: T => Map[String, AnyRef]): ParametersBuilder[T] = f(_)

}
