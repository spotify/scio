/*
 * Copyright 2019 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.scio.coders.instances

import com.spotify.scio.coders.{Coder, CoderGrammar}
import com.twitter.algebird.{BF, Batched, CMS, Moments, TopCMS, TopK}

trait AlgebirdCoders extends CoderGrammar {
  private lazy val kryoCmsCoder: Coder[CMS[_]] = kryo
  private lazy val kryoTopCmsCoder: Coder[TopCMS[_]] = kryo
  private lazy val kryoBfCoder: Coder[BF[_]] = kryo
  private lazy val kryoTopKCoder: Coder[TopK[_]] = kryo
  private lazy val kryoBatchedCoder: Coder[Batched[_]] = kryo

  implicit def cmsCoder[T]: Coder[CMS[T]] = kryoCmsCoder.asInstanceOf[Coder[CMS[T]]]
  implicit def topCmsCoder[T]: Coder[TopCMS[T]] = kryoTopCmsCoder.asInstanceOf[Coder[TopCMS[T]]]
  implicit def bfCoder[T]: Coder[BF[T]] = kryoBfCoder.asInstanceOf[Coder[BF[T]]]
  implicit def topKCoder[T]: Coder[TopK[T]] = kryoTopKCoder.asInstanceOf[Coder[TopK[T]]]
  implicit def batchedCoder[T]: Coder[Batched[T]] = kryoBatchedCoder.asInstanceOf[Coder[Batched[T]]]

  implicit lazy val momentsCoder: Coder[Moments] =
    xmap(Coder[(Double, Double, Double, Double, Double)])(
      { case (m0D, m1, m2, m3, m4) => new Moments(m0D, m1, m2, m3, m4) },
      m => (m.m0D, m.m1, m.m2, m.m3, m.m4)
    )
}

private[coders] object AlgebirdCoders extends AlgebirdCoders
