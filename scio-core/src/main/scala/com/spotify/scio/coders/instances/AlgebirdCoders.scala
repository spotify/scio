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

import com.spotify.scio.coders.Coder
import com.twitter.algebird.{BF, Batched, CMS, Moments, TopCMS, TopK}

trait AlgebirdCoders {
  implicit def cmsCoder[K]: Coder[CMS[K]] = Coder.kryo
  implicit def topCmsCoder[K]: Coder[TopCMS[K]] = Coder.kryo
  implicit def bfCoder[K]: Coder[BF[K]] = Coder.kryo
  implicit def topKCoder[K]: Coder[TopK[K]] = Coder.kryo
  implicit def batchedCoder[U]: Coder[Batched[U]] = Coder.kryo
  implicit def momentsCoder[U]: Coder[Moments] =
    Coder.xmap(Coder[(Double, Double, Double, Double, Double)])(
      { case (m0D, m1, m2, m3, m4) => new Moments(m0D, m1, m2, m3, m4) },
      m => (m.m0D, m.m1, m.m2, m.m3, m.m4)
    )
}
