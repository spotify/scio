package kantan.codecs

import kantan.codecs.resource.ResourceIterator

object compat {

  implicit class ResourceIteratorCompatOps[T](private val underlying: ResourceIterator[T])
      extends AnyVal {
    def iterator: Iterator[T] = underlying.toIterator
  }

}
