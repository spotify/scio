/*
 * Copyright 2020 Spotify AB.
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

// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
// !! generated with tuplecoders.py
// !! DO NOT EDIT MANUALLY
// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!


package com.spotify.scio.coders.instances

import com.spotify.scio.coders.Coder

trait TupleCoders {


    implicit def tuple3Coder[A: Coder, B: Coder, C: Coder]: Coder[(A, B, C)] = {
      Coder.gen[(A, B, C)]
    }

    implicit def tuple4Coder[A: Coder, B: Coder, C: Coder, D: Coder]: Coder[(A, B, C, D)] = {
      Coder.gen[(A, B, C, D)]
    }

    implicit def tuple5Coder[A: Coder, B: Coder, C: Coder, D: Coder, E: Coder]: Coder[(A, B, C, D, E)] = {
      Coder.gen[(A, B, C, D, E)]
    }

    implicit def tuple6Coder[A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, G: Coder]: Coder[(A, B, C, D, E, G)] = {
      Coder.gen[(A, B, C, D, E, G)]
    }

    implicit def tuple7Coder[A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, G: Coder, H: Coder]: Coder[(A, B, C, D, E, G, H)] = {
      Coder.gen[(A, B, C, D, E, G, H)]
    }

    implicit def tuple8Coder[A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, G: Coder, H: Coder, I: Coder]: Coder[(A, B, C, D, E, G, H, I)] = {
      Coder.gen[(A, B, C, D, E, G, H, I)]
    }

    implicit def tuple9Coder[A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, G: Coder, H: Coder, I: Coder, J: Coder]: Coder[(A, B, C, D, E, G, H, I, J)] = {
      Coder.gen[(A, B, C, D, E, G, H, I, J)]
    }

    implicit def tuple10Coder[A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder]: Coder[(A, B, C, D, E, G, H, I, J, K)] = {
      Coder.gen[(A, B, C, D, E, G, H, I, J, K)]
    }

    implicit def tuple11Coder[A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder]: Coder[(A, B, C, D, E, G, H, I, J, K, L)] = {
      Coder.gen[(A, B, C, D, E, G, H, I, J, K, L)]
    }

    implicit def tuple12Coder[A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder]: Coder[(A, B, C, D, E, G, H, I, J, K, L, M)] = {
      Coder.gen[(A, B, C, D, E, G, H, I, J, K, L, M)]
    }

    implicit def tuple13Coder[A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder, N: Coder]: Coder[(A, B, C, D, E, G, H, I, J, K, L, M, N)] = {
      Coder.gen[(A, B, C, D, E, G, H, I, J, K, L, M, N)]
    }

    implicit def tuple14Coder[A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder, N: Coder, O: Coder]: Coder[(A, B, C, D, E, G, H, I, J, K, L, M, N, O)] = {
      Coder.gen[(A, B, C, D, E, G, H, I, J, K, L, M, N, O)]
    }

    implicit def tuple15Coder[A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder, N: Coder, O: Coder, P: Coder]: Coder[(A, B, C, D, E, G, H, I, J, K, L, M, N, O, P)] = {
      Coder.gen[(A, B, C, D, E, G, H, I, J, K, L, M, N, O, P)]
    }

    implicit def tuple16Coder[A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder, N: Coder, O: Coder, P: Coder, Q: Coder]: Coder[(A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q)] = {
      Coder.gen[(A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q)]
    }

    implicit def tuple17Coder[A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder, N: Coder, O: Coder, P: Coder, Q: Coder, R: Coder]: Coder[(A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R)] = {
      Coder.gen[(A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R)]
    }

    implicit def tuple18Coder[A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder, N: Coder, O: Coder, P: Coder, Q: Coder, R: Coder, S: Coder]: Coder[(A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R, S)] = {
      Coder.gen[(A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R, S)]
    }

    implicit def tuple19Coder[A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder, N: Coder, O: Coder, P: Coder, Q: Coder, R: Coder, S: Coder, T: Coder]: Coder[(A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R, S, T)] = {
      Coder.gen[(A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R, S, T)]
    }

    implicit def tuple20Coder[A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder, N: Coder, O: Coder, P: Coder, Q: Coder, R: Coder, S: Coder, T: Coder, U: Coder]: Coder[(A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U)] = {
      Coder.gen[(A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U)]
    }

    implicit def tuple21Coder[A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder, N: Coder, O: Coder, P: Coder, Q: Coder, R: Coder, S: Coder, T: Coder, U: Coder, V: Coder]: Coder[(A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V)] = {
      Coder.gen[(A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V)]
    }

    implicit def tuple22Coder[A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder, N: Coder, O: Coder, P: Coder, Q: Coder, R: Coder, S: Coder, T: Coder, U: Coder, V: Coder, W: Coder]: Coder[(A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V, W)] = {
      Coder.gen[(A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V, W)]
    }
}
