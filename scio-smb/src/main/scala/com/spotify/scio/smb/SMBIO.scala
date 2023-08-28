package com.spotify.scio.smb

import com.spotify.scio.io.{KeyedIO, TapOf, TapT, TestIO}

final case class SMBIO[K, T](id: String, keyBy: T => K) extends KeyedIO[K, T] with TestIO[(K, T)] {
  override val tapT: TapT.Aux[(K, T), (K, T)] = TapOf[(K, T)]
  override def testId: String = s"SMBIO($id)"
}
