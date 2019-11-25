package com.spotify.scio.smb.io

import com.spotify.scio.io.{EmptyTapOf, TestIO}

final case class SortMergeBucketRead[K, T](override val testId: String) extends TestIO[(K, T)] {
  override val tapT = EmptyTapOf[(K, T)]
}

final case class SortMergeBucketWrite[T](override val testId: String) extends TestIO[T] {
  override val tapT = EmptyTapOf[T]
}
