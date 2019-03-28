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

package com.spotify.scio.coders.instances

import com.spotify.scio.coders.Coder
import shapeless.Strict

trait TupleCoders {


    implicit def tuple3Coder[A, B, C](implicit A: Strict[Coder[A]], B: Strict[Coder[B]], C: Strict[Coder[C]]): Coder[(A, B, C)] = {
      implicit val xA = A.value
      implicit val xB = B.value
      implicit val xC = C.value
      Coder.gen[(A, B, C)]
    }

    implicit def tuple4Coder[A, B, C, D](implicit A: Strict[Coder[A]], B: Strict[Coder[B]], C: Strict[Coder[C]], D: Strict[Coder[D]]): Coder[(A, B, C, D)] = {
      implicit val xA = A.value
      implicit val xB = B.value
      implicit val xC = C.value
      implicit val xD = D.value
      Coder.gen[(A, B, C, D)]
    }

    implicit def tuple5Coder[A, B, C, D, E](implicit A: Strict[Coder[A]], B: Strict[Coder[B]], C: Strict[Coder[C]], D: Strict[Coder[D]], E: Strict[Coder[E]]): Coder[(A, B, C, D, E)] = {
      implicit val xA = A.value
      implicit val xB = B.value
      implicit val xC = C.value
      implicit val xD = D.value
      implicit val xE = E.value
      Coder.gen[(A, B, C, D, E)]
    }

    implicit def tuple6Coder[A, B, C, D, E, G](implicit A: Strict[Coder[A]], B: Strict[Coder[B]], C: Strict[Coder[C]], D: Strict[Coder[D]], E: Strict[Coder[E]], G: Strict[Coder[G]]): Coder[(A, B, C, D, E, G)] = {
      implicit val xA = A.value
      implicit val xB = B.value
      implicit val xC = C.value
      implicit val xD = D.value
      implicit val xE = E.value
      implicit val xG = G.value
      Coder.gen[(A, B, C, D, E, G)]
    }

    implicit def tuple7Coder[A, B, C, D, E, G, H](implicit A: Strict[Coder[A]], B: Strict[Coder[B]], C: Strict[Coder[C]], D: Strict[Coder[D]], E: Strict[Coder[E]], G: Strict[Coder[G]], H: Strict[Coder[H]]): Coder[(A, B, C, D, E, G, H)] = {
      implicit val xA = A.value
      implicit val xB = B.value
      implicit val xC = C.value
      implicit val xD = D.value
      implicit val xE = E.value
      implicit val xG = G.value
      implicit val xH = H.value
      Coder.gen[(A, B, C, D, E, G, H)]
    }

    implicit def tuple8Coder[A, B, C, D, E, G, H, I](implicit A: Strict[Coder[A]], B: Strict[Coder[B]], C: Strict[Coder[C]], D: Strict[Coder[D]], E: Strict[Coder[E]], G: Strict[Coder[G]], H: Strict[Coder[H]], I: Strict[Coder[I]]): Coder[(A, B, C, D, E, G, H, I)] = {
      implicit val xA = A.value
      implicit val xB = B.value
      implicit val xC = C.value
      implicit val xD = D.value
      implicit val xE = E.value
      implicit val xG = G.value
      implicit val xH = H.value
      implicit val xI = I.value
      Coder.gen[(A, B, C, D, E, G, H, I)]
    }

    implicit def tuple9Coder[A, B, C, D, E, G, H, I, J](implicit A: Strict[Coder[A]], B: Strict[Coder[B]], C: Strict[Coder[C]], D: Strict[Coder[D]], E: Strict[Coder[E]], G: Strict[Coder[G]], H: Strict[Coder[H]], I: Strict[Coder[I]], J: Strict[Coder[J]]): Coder[(A, B, C, D, E, G, H, I, J)] = {
      implicit val xA = A.value
      implicit val xB = B.value
      implicit val xC = C.value
      implicit val xD = D.value
      implicit val xE = E.value
      implicit val xG = G.value
      implicit val xH = H.value
      implicit val xI = I.value
      implicit val xJ = J.value
      Coder.gen[(A, B, C, D, E, G, H, I, J)]
    }

    implicit def tuple10Coder[A, B, C, D, E, G, H, I, J, K](implicit A: Strict[Coder[A]], B: Strict[Coder[B]], C: Strict[Coder[C]], D: Strict[Coder[D]], E: Strict[Coder[E]], G: Strict[Coder[G]], H: Strict[Coder[H]], I: Strict[Coder[I]], J: Strict[Coder[J]], K: Strict[Coder[K]]): Coder[(A, B, C, D, E, G, H, I, J, K)] = {
      implicit val xA = A.value
      implicit val xB = B.value
      implicit val xC = C.value
      implicit val xD = D.value
      implicit val xE = E.value
      implicit val xG = G.value
      implicit val xH = H.value
      implicit val xI = I.value
      implicit val xJ = J.value
      implicit val xK = K.value
      Coder.gen[(A, B, C, D, E, G, H, I, J, K)]
    }

    implicit def tuple11Coder[A, B, C, D, E, G, H, I, J, K, L](implicit A: Strict[Coder[A]], B: Strict[Coder[B]], C: Strict[Coder[C]], D: Strict[Coder[D]], E: Strict[Coder[E]], G: Strict[Coder[G]], H: Strict[Coder[H]], I: Strict[Coder[I]], J: Strict[Coder[J]], K: Strict[Coder[K]], L: Strict[Coder[L]]): Coder[(A, B, C, D, E, G, H, I, J, K, L)] = {
      implicit val xA = A.value
      implicit val xB = B.value
      implicit val xC = C.value
      implicit val xD = D.value
      implicit val xE = E.value
      implicit val xG = G.value
      implicit val xH = H.value
      implicit val xI = I.value
      implicit val xJ = J.value
      implicit val xK = K.value
      implicit val xL = L.value
      Coder.gen[(A, B, C, D, E, G, H, I, J, K, L)]
    }

    implicit def tuple12Coder[A, B, C, D, E, G, H, I, J, K, L, M](implicit A: Strict[Coder[A]], B: Strict[Coder[B]], C: Strict[Coder[C]], D: Strict[Coder[D]], E: Strict[Coder[E]], G: Strict[Coder[G]], H: Strict[Coder[H]], I: Strict[Coder[I]], J: Strict[Coder[J]], K: Strict[Coder[K]], L: Strict[Coder[L]], M: Strict[Coder[M]]): Coder[(A, B, C, D, E, G, H, I, J, K, L, M)] = {
      implicit val xA = A.value
      implicit val xB = B.value
      implicit val xC = C.value
      implicit val xD = D.value
      implicit val xE = E.value
      implicit val xG = G.value
      implicit val xH = H.value
      implicit val xI = I.value
      implicit val xJ = J.value
      implicit val xK = K.value
      implicit val xL = L.value
      implicit val xM = M.value
      Coder.gen[(A, B, C, D, E, G, H, I, J, K, L, M)]
    }

    implicit def tuple13Coder[A, B, C, D, E, G, H, I, J, K, L, M, N](implicit A: Strict[Coder[A]], B: Strict[Coder[B]], C: Strict[Coder[C]], D: Strict[Coder[D]], E: Strict[Coder[E]], G: Strict[Coder[G]], H: Strict[Coder[H]], I: Strict[Coder[I]], J: Strict[Coder[J]], K: Strict[Coder[K]], L: Strict[Coder[L]], M: Strict[Coder[M]], N: Strict[Coder[N]]): Coder[(A, B, C, D, E, G, H, I, J, K, L, M, N)] = {
      implicit val xA = A.value
      implicit val xB = B.value
      implicit val xC = C.value
      implicit val xD = D.value
      implicit val xE = E.value
      implicit val xG = G.value
      implicit val xH = H.value
      implicit val xI = I.value
      implicit val xJ = J.value
      implicit val xK = K.value
      implicit val xL = L.value
      implicit val xM = M.value
      implicit val xN = N.value
      Coder.gen[(A, B, C, D, E, G, H, I, J, K, L, M, N)]
    }

    implicit def tuple14Coder[A, B, C, D, E, G, H, I, J, K, L, M, N, O](implicit A: Strict[Coder[A]], B: Strict[Coder[B]], C: Strict[Coder[C]], D: Strict[Coder[D]], E: Strict[Coder[E]], G: Strict[Coder[G]], H: Strict[Coder[H]], I: Strict[Coder[I]], J: Strict[Coder[J]], K: Strict[Coder[K]], L: Strict[Coder[L]], M: Strict[Coder[M]], N: Strict[Coder[N]], O: Strict[Coder[O]]): Coder[(A, B, C, D, E, G, H, I, J, K, L, M, N, O)] = {
      implicit val xA = A.value
      implicit val xB = B.value
      implicit val xC = C.value
      implicit val xD = D.value
      implicit val xE = E.value
      implicit val xG = G.value
      implicit val xH = H.value
      implicit val xI = I.value
      implicit val xJ = J.value
      implicit val xK = K.value
      implicit val xL = L.value
      implicit val xM = M.value
      implicit val xN = N.value
      implicit val xO = O.value
      Coder.gen[(A, B, C, D, E, G, H, I, J, K, L, M, N, O)]
    }

    implicit def tuple15Coder[A, B, C, D, E, G, H, I, J, K, L, M, N, O, P](implicit A: Strict[Coder[A]], B: Strict[Coder[B]], C: Strict[Coder[C]], D: Strict[Coder[D]], E: Strict[Coder[E]], G: Strict[Coder[G]], H: Strict[Coder[H]], I: Strict[Coder[I]], J: Strict[Coder[J]], K: Strict[Coder[K]], L: Strict[Coder[L]], M: Strict[Coder[M]], N: Strict[Coder[N]], O: Strict[Coder[O]], P: Strict[Coder[P]]): Coder[(A, B, C, D, E, G, H, I, J, K, L, M, N, O, P)] = {
      implicit val xA = A.value
      implicit val xB = B.value
      implicit val xC = C.value
      implicit val xD = D.value
      implicit val xE = E.value
      implicit val xG = G.value
      implicit val xH = H.value
      implicit val xI = I.value
      implicit val xJ = J.value
      implicit val xK = K.value
      implicit val xL = L.value
      implicit val xM = M.value
      implicit val xN = N.value
      implicit val xO = O.value
      implicit val xP = P.value
      Coder.gen[(A, B, C, D, E, G, H, I, J, K, L, M, N, O, P)]
    }

    implicit def tuple16Coder[A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q](implicit A: Strict[Coder[A]], B: Strict[Coder[B]], C: Strict[Coder[C]], D: Strict[Coder[D]], E: Strict[Coder[E]], G: Strict[Coder[G]], H: Strict[Coder[H]], I: Strict[Coder[I]], J: Strict[Coder[J]], K: Strict[Coder[K]], L: Strict[Coder[L]], M: Strict[Coder[M]], N: Strict[Coder[N]], O: Strict[Coder[O]], P: Strict[Coder[P]], Q: Strict[Coder[Q]]): Coder[(A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q)] = {
      implicit val xA = A.value
      implicit val xB = B.value
      implicit val xC = C.value
      implicit val xD = D.value
      implicit val xE = E.value
      implicit val xG = G.value
      implicit val xH = H.value
      implicit val xI = I.value
      implicit val xJ = J.value
      implicit val xK = K.value
      implicit val xL = L.value
      implicit val xM = M.value
      implicit val xN = N.value
      implicit val xO = O.value
      implicit val xP = P.value
      implicit val xQ = Q.value
      Coder.gen[(A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q)]
    }

    implicit def tuple17Coder[A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R](implicit A: Strict[Coder[A]], B: Strict[Coder[B]], C: Strict[Coder[C]], D: Strict[Coder[D]], E: Strict[Coder[E]], G: Strict[Coder[G]], H: Strict[Coder[H]], I: Strict[Coder[I]], J: Strict[Coder[J]], K: Strict[Coder[K]], L: Strict[Coder[L]], M: Strict[Coder[M]], N: Strict[Coder[N]], O: Strict[Coder[O]], P: Strict[Coder[P]], Q: Strict[Coder[Q]], R: Strict[Coder[R]]): Coder[(A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R)] = {
      implicit val xA = A.value
      implicit val xB = B.value
      implicit val xC = C.value
      implicit val xD = D.value
      implicit val xE = E.value
      implicit val xG = G.value
      implicit val xH = H.value
      implicit val xI = I.value
      implicit val xJ = J.value
      implicit val xK = K.value
      implicit val xL = L.value
      implicit val xM = M.value
      implicit val xN = N.value
      implicit val xO = O.value
      implicit val xP = P.value
      implicit val xQ = Q.value
      implicit val xR = R.value
      Coder.gen[(A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R)]
    }

    implicit def tuple18Coder[A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R, S](implicit A: Strict[Coder[A]], B: Strict[Coder[B]], C: Strict[Coder[C]], D: Strict[Coder[D]], E: Strict[Coder[E]], G: Strict[Coder[G]], H: Strict[Coder[H]], I: Strict[Coder[I]], J: Strict[Coder[J]], K: Strict[Coder[K]], L: Strict[Coder[L]], M: Strict[Coder[M]], N: Strict[Coder[N]], O: Strict[Coder[O]], P: Strict[Coder[P]], Q: Strict[Coder[Q]], R: Strict[Coder[R]], S: Strict[Coder[S]]): Coder[(A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R, S)] = {
      implicit val xA = A.value
      implicit val xB = B.value
      implicit val xC = C.value
      implicit val xD = D.value
      implicit val xE = E.value
      implicit val xG = G.value
      implicit val xH = H.value
      implicit val xI = I.value
      implicit val xJ = J.value
      implicit val xK = K.value
      implicit val xL = L.value
      implicit val xM = M.value
      implicit val xN = N.value
      implicit val xO = O.value
      implicit val xP = P.value
      implicit val xQ = Q.value
      implicit val xR = R.value
      implicit val xS = S.value
      Coder.gen[(A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R, S)]
    }

    implicit def tuple19Coder[A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R, S, T](implicit A: Strict[Coder[A]], B: Strict[Coder[B]], C: Strict[Coder[C]], D: Strict[Coder[D]], E: Strict[Coder[E]], G: Strict[Coder[G]], H: Strict[Coder[H]], I: Strict[Coder[I]], J: Strict[Coder[J]], K: Strict[Coder[K]], L: Strict[Coder[L]], M: Strict[Coder[M]], N: Strict[Coder[N]], O: Strict[Coder[O]], P: Strict[Coder[P]], Q: Strict[Coder[Q]], R: Strict[Coder[R]], S: Strict[Coder[S]], T: Strict[Coder[T]]): Coder[(A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R, S, T)] = {
      implicit val xA = A.value
      implicit val xB = B.value
      implicit val xC = C.value
      implicit val xD = D.value
      implicit val xE = E.value
      implicit val xG = G.value
      implicit val xH = H.value
      implicit val xI = I.value
      implicit val xJ = J.value
      implicit val xK = K.value
      implicit val xL = L.value
      implicit val xM = M.value
      implicit val xN = N.value
      implicit val xO = O.value
      implicit val xP = P.value
      implicit val xQ = Q.value
      implicit val xR = R.value
      implicit val xS = S.value
      implicit val xT = T.value
      Coder.gen[(A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R, S, T)]
    }

    implicit def tuple20Coder[A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U](implicit A: Strict[Coder[A]], B: Strict[Coder[B]], C: Strict[Coder[C]], D: Strict[Coder[D]], E: Strict[Coder[E]], G: Strict[Coder[G]], H: Strict[Coder[H]], I: Strict[Coder[I]], J: Strict[Coder[J]], K: Strict[Coder[K]], L: Strict[Coder[L]], M: Strict[Coder[M]], N: Strict[Coder[N]], O: Strict[Coder[O]], P: Strict[Coder[P]], Q: Strict[Coder[Q]], R: Strict[Coder[R]], S: Strict[Coder[S]], T: Strict[Coder[T]], U: Strict[Coder[U]]): Coder[(A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U)] = {
      implicit val xA = A.value
      implicit val xB = B.value
      implicit val xC = C.value
      implicit val xD = D.value
      implicit val xE = E.value
      implicit val xG = G.value
      implicit val xH = H.value
      implicit val xI = I.value
      implicit val xJ = J.value
      implicit val xK = K.value
      implicit val xL = L.value
      implicit val xM = M.value
      implicit val xN = N.value
      implicit val xO = O.value
      implicit val xP = P.value
      implicit val xQ = Q.value
      implicit val xR = R.value
      implicit val xS = S.value
      implicit val xT = T.value
      implicit val xU = U.value
      Coder.gen[(A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U)]
    }

    implicit def tuple21Coder[A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V](implicit A: Strict[Coder[A]], B: Strict[Coder[B]], C: Strict[Coder[C]], D: Strict[Coder[D]], E: Strict[Coder[E]], G: Strict[Coder[G]], H: Strict[Coder[H]], I: Strict[Coder[I]], J: Strict[Coder[J]], K: Strict[Coder[K]], L: Strict[Coder[L]], M: Strict[Coder[M]], N: Strict[Coder[N]], O: Strict[Coder[O]], P: Strict[Coder[P]], Q: Strict[Coder[Q]], R: Strict[Coder[R]], S: Strict[Coder[S]], T: Strict[Coder[T]], U: Strict[Coder[U]], V: Strict[Coder[V]]): Coder[(A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V)] = {
      implicit val xA = A.value
      implicit val xB = B.value
      implicit val xC = C.value
      implicit val xD = D.value
      implicit val xE = E.value
      implicit val xG = G.value
      implicit val xH = H.value
      implicit val xI = I.value
      implicit val xJ = J.value
      implicit val xK = K.value
      implicit val xL = L.value
      implicit val xM = M.value
      implicit val xN = N.value
      implicit val xO = O.value
      implicit val xP = P.value
      implicit val xQ = Q.value
      implicit val xR = R.value
      implicit val xS = S.value
      implicit val xT = T.value
      implicit val xU = U.value
      implicit val xV = V.value
      Coder.gen[(A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V)]
    }

    implicit def tuple22Coder[A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V, W](implicit A: Strict[Coder[A]], B: Strict[Coder[B]], C: Strict[Coder[C]], D: Strict[Coder[D]], E: Strict[Coder[E]], G: Strict[Coder[G]], H: Strict[Coder[H]], I: Strict[Coder[I]], J: Strict[Coder[J]], K: Strict[Coder[K]], L: Strict[Coder[L]], M: Strict[Coder[M]], N: Strict[Coder[N]], O: Strict[Coder[O]], P: Strict[Coder[P]], Q: Strict[Coder[Q]], R: Strict[Coder[R]], S: Strict[Coder[S]], T: Strict[Coder[T]], U: Strict[Coder[U]], V: Strict[Coder[V]], W: Strict[Coder[W]]): Coder[(A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V, W)] = {
      implicit val xA = A.value
      implicit val xB = B.value
      implicit val xC = C.value
      implicit val xD = D.value
      implicit val xE = E.value
      implicit val xG = G.value
      implicit val xH = H.value
      implicit val xI = I.value
      implicit val xJ = J.value
      implicit val xK = K.value
      implicit val xL = L.value
      implicit val xM = M.value
      implicit val xN = N.value
      implicit val xO = O.value
      implicit val xP = P.value
      implicit val xQ = Q.value
      implicit val xR = R.value
      implicit val xS = S.value
      implicit val xT = T.value
      implicit val xU = U.value
      implicit val xV = V.value
      implicit val xW = W.value
      Coder.gen[(A, B, C, D, E, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V, W)]
    }
}

// scalastyle:on cyclomatic.complexity
// scalastyle:on file.size.limit
// scalastyle:on line.size.limit
// scalastyle:on method.length
// scalastyle:on number.of.methods
// scalastyle:on parameter.number
