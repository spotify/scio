package com.spotify.scio.coders.instances

import com.spotify.scio.coders.Coder
import com.twitter.algebird.{BF, Batched, CMS, TopK}

trait AlgebirdCoders {
  implicit def cmsCoder[K]: Coder[CMS[K]] = Coder.kryo
  implicit def bfCoder[K]: Coder[BF[K]] = Coder.kryo
  implicit def topKCoder[K]: Coder[TopK[K]] = Coder.kryo
  implicit def batchedCoder[U]: Coder[Batched[U]] = Coder.kryo
}
