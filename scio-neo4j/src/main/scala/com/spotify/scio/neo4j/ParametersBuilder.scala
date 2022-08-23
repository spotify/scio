package com.spotify.scio.neo4j

/**
 * Type class for creating a map of parameters to be used when running parametrized cypher queries
 * <p> Allowed parameter types are: <ul> <li>[[java.lang.Integer]]</li> <li>[[java.lang.Long]]</li>
 * <li>[[java.lang.Boolean]]</li> <li>[[java.lang.Double]]</li> <li>[[java.lang.Float]]</li>
 * <li>[[java.lang.String]]</li> <li>[[java.util.Map]] with String keys and values being any type in
 * this list</li> <li>[[java.util.Collection]] of any type in this list</li> </ul>
 */
trait ParametersBuilder[T] extends Serializable {
  def apply(x: T): Map[String, AnyRef]
}

object ParametersBuilder {
  def apply[T](implicit instance: ParametersBuilder[T]): ParametersBuilder[T] = instance
}
