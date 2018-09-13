/*
 * Copyright 2016 Spotify AB.
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

// scalastyle:off cyclomatic.complexity
// scalastyle:off file.size.limit
// scalastyle:off line.size.limit
// scalastyle:off method.length
// scalastyle:off number.of.methods
// scalastyle:off parameter.number

package com.spotify.scio.coders

trait TupleCoders {

  implicit def tuple2Coder[A, B](implicit A: Coder[A], B: Coder[B]): Coder[(A, B)] = Coder.gen[(A, B)]
  implicit def tuple3Coder[A, B, C](implicit A: Coder[A], B: Coder[B], C: Coder[C]): Coder[(A, B, C)] = Coder.gen[(A, B, C)]
  implicit def tuple4Coder[A, B, C, D](implicit A: Coder[A], B: Coder[B], C: Coder[C], D: Coder[D]): Coder[(A, B, C, D)] = Coder.gen[(A, B, C, D)]
  implicit def tuple5Coder[A, B, C, D, E](implicit A: Coder[A], B: Coder[B], C: Coder[C], D: Coder[D], E: Coder[E]): Coder[(A, B, C, D, E)] = Coder.gen[(A, B, C, D, E)]
  implicit def tuple6Coder[A, B, C, D, E, G](implicit A: Coder[A], B: Coder[B], C: Coder[C], D: Coder[D], E: Coder[E], G: Coder[G]): Coder[(A, B, C, D, E, G)] = Coder.gen[(A, B, C, D, E, G)]
  implicit def tuple7Coder[A, B, C, D, E, G, H](implicit A: Coder[A], B: Coder[B], C: Coder[C], D: Coder[D], E: Coder[E], G: Coder[G], H: Coder[H]): Coder[(A, B, C, D, E, G, H)] = Coder.gen[(A, B, C, D, E, G, H)]
  implicit def tuple8Coder[A, B, C, D, E, G, H, I](implicit A: Coder[A], B: Coder[B], C: Coder[C], D: Coder[D], E: Coder[E], G: Coder[G], H: Coder[H], I: Coder[I]): Coder[(A, B, C, D, E, G, H, I)] = Coder.gen[(A, B, C, D, E, G, H, I)]
  implicit def tuple9Coder[A, B, C, D, E, G, H, I, J](implicit A: Coder[A], B: Coder[B], C: Coder[C], D: Coder[D], E: Coder[E], G: Coder[G], H: Coder[H], I: Coder[I], J: Coder[J]): Coder[(A, B, C, D, E, G, H, I, J)] = Coder.gen[(A, B, C, D, E, G, H, I, J)]
  implicit def tuple10Coder[A, B, C, D, E, G, H, I, J, K](implicit A: Coder[A], B: Coder[B], C: Coder[C], D: Coder[D], E: Coder[E], G: Coder[G], H: Coder[H], I: Coder[I], J: Coder[J], K: Coder[K]): Coder[(A, B, C, D, E, G, H, I, J, K)] = Coder.gen[(A, B, C, D, E, G, H, I, J, K)]
  implicit def tuple11Coder[A, B, C, D, E, G, H, I, J, K, L](implicit A: Coder[A], B: Coder[B], C: Coder[C], D: Coder[D], E: Coder[E], G: Coder[G], H: Coder[H], I: Coder[I], J: Coder[J], K: Coder[K], L: Coder[L]): Coder[(A, B, C, D, E, G, H, I, J, K, L)] = Coder.gen[(A, B, C, D, E, G, H, I, J, K, L)]
  implicit def tuple12Coder[A, B, C, D, E, G, H, I, J, K, L, M](implicit A: Coder[A], B: Coder[B], C: Coder[C], D: Coder[D], E: Coder[E], G: Coder[G], H: Coder[H], I: Coder[I], J: Coder[J], K: Coder[K], L: Coder[L], M: Coder[M]): Coder[(A, B, C, D, E, G, H, I, J, K, L, M)] = Coder.gen[(A, B, C, D, E, G, H, I, J, K, L, M)]
  implicit def tuple13Coder[A, B, C, D, E, G, H, I, J, K, L, M, N](implicit A: Coder[A], B: Coder[B], C: Coder[C], D: Coder[D], E: Coder[E], G: Coder[G], H: Coder[H], I: Coder[I], J: Coder[J], K: Coder[K], L: Coder[L], M: Coder[M], N: Coder[N]): Coder[(A, B, C, D, E, G, H, I, J, K, L, M, N)] = Coder.gen[(A, B, C, D, E, G, H, I, J, K, L, M, N)]
  implicit def tuple14Coder[A, B, C, D, E, G, H, I, J, K, L, M, N, O](implicit A: Coder[A], B: Coder[B], C: Coder[C], D: Coder[D], E: Coder[E], G: Coder[G], H: Coder[H], I: Coder[I], J: Coder[J], K: Coder[K], L: Coder[L], M: Coder[M], N: Coder[N], O: Coder[O]): Coder[(A, B, C, D, E, G, H, I, J, K, L, M, N, O)] = Coder.gen[(A, B, C, D, E, G, H, I, J, K, L, M, N, O)]
  implicit def tuple15Coder[A, B, C, D, E, G, H, I, J, K, L, M, N, O, P](implicit A: Coder[A], B: Coder[B], C: Coder[C], D: Coder[D], E: Coder[E], G: Coder[G], H: Coder[H], I: Coder[I], J: Coder[J], K: Coder[K], L: Coder[L], M: Coder[M], N: Coder[N], O: Coder[O], P: Coder[P]): Coder[(A, B, C, D, E, G, H, I, J, K, L, M, N, O, P)] = Coder.gen[(A, B, C, D, E, G, H, I, J, K, L, M, N, O, P)]
  implicit def tuple16Coder[A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q](implicit A: Coder[A], B: Coder[B], C: Coder[C], D: Coder[D], E: Coder[E], G: Coder[G], H: Coder[H], I: Coder[I], J: Coder[J], K: Coder[K], L: Coder[L], M: Coder[M], N: Coder[N], O: Coder[O], P: Coder[P], Q: Coder[Q]): Coder[(A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q)] = Coder.gen[(A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q)]
  implicit def tuple17Coder[A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R](implicit A: Coder[A], B: Coder[B], C: Coder[C], D: Coder[D], E: Coder[E], G: Coder[G], H: Coder[H], I: Coder[I], J: Coder[J], K: Coder[K], L: Coder[L], M: Coder[M], N: Coder[N], O: Coder[O], P: Coder[P], Q: Coder[Q], R: Coder[R]): Coder[(A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R)] = Coder.gen[(A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R)]
  implicit def tuple18Coder[A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R, S](implicit A: Coder[A], B: Coder[B], C: Coder[C], D: Coder[D], E: Coder[E], G: Coder[G], H: Coder[H], I: Coder[I], J: Coder[J], K: Coder[K], L: Coder[L], M: Coder[M], N: Coder[N], O: Coder[O], P: Coder[P], Q: Coder[Q], R: Coder[R], S: Coder[S]): Coder[(A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R, S)] = Coder.gen[(A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R, S)]
  implicit def tuple19Coder[A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R, S, T](implicit A: Coder[A], B: Coder[B], C: Coder[C], D: Coder[D], E: Coder[E], G: Coder[G], H: Coder[H], I: Coder[I], J: Coder[J], K: Coder[K], L: Coder[L], M: Coder[M], N: Coder[N], O: Coder[O], P: Coder[P], Q: Coder[Q], R: Coder[R], S: Coder[S], T: Coder[T]): Coder[(A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R, S, T)] = Coder.gen[(A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R, S, T)]
  implicit def tuple20Coder[A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U](implicit A: Coder[A], B: Coder[B], C: Coder[C], D: Coder[D], E: Coder[E], G: Coder[G], H: Coder[H], I: Coder[I], J: Coder[J], K: Coder[K], L: Coder[L], M: Coder[M], N: Coder[N], O: Coder[O], P: Coder[P], Q: Coder[Q], R: Coder[R], S: Coder[S], T: Coder[T], U: Coder[U]): Coder[(A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U)] = Coder.gen[(A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U)]
  implicit def tuple21Coder[A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V](implicit A: Coder[A], B: Coder[B], C: Coder[C], D: Coder[D], E: Coder[E], G: Coder[G], H: Coder[H], I: Coder[I], J: Coder[J], K: Coder[K], L: Coder[L], M: Coder[M], N: Coder[N], O: Coder[O], P: Coder[P], Q: Coder[Q], R: Coder[R], S: Coder[S], T: Coder[T], U: Coder[U], V: Coder[V]): Coder[(A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V)] = Coder.gen[(A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V)]
  implicit def tuple22Coder[A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V, W](implicit A: Coder[A], B: Coder[B], C: Coder[C], D: Coder[D], E: Coder[E], G: Coder[G], H: Coder[H], I: Coder[I], J: Coder[J], K: Coder[K], L: Coder[L], M: Coder[M], N: Coder[N], O: Coder[O], P: Coder[P], Q: Coder[Q], R: Coder[R], S: Coder[S], T: Coder[T], U: Coder[U], V: Coder[V], W: Coder[W]): Coder[(A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V, W)] = Coder.gen[(A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V, W)]
}

// scalastyle:on cyclomatic.complexity
// scalastyle:on file.size.limit
// scalastyle:on line.size.limit
// scalastyle:on method.length
// scalastyle:on number.of.methods
// scalastyle:on parameter.number
