package scala.collection.compat

object extra {
  type Wrappers = scala.collection.convert.JavaCollectionWrappers.type
  val Wrappers = scala.collection.convert.JavaCollectionWrappers

  val CollectionConverters = scala.jdk.javaapi.CollectionConverters
}
