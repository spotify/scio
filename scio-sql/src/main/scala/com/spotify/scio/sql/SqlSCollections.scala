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

// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
// !! generated with sql.py
// !! DO NOT EDIT MANUALLY
// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

package com.spotify.scio.sql

import com.spotify.scio.schemas._
import com.spotify.scio.values.SCollection

trait SqlSCollections {
  def from[A: Schema](a: SCollection[A]): SqlSCollection1[A] = new SqlSCollection1(a)
  def from[A: Schema, B: Schema](a: SCollection[A], b: SCollection[B]): SqlSCollection2[A, B] =
    new SqlSCollection2(a, b)
  def from[A: Schema, B: Schema, C: Schema](
    a: SCollection[A],
    b: SCollection[B],
    c: SCollection[C]
  ): SqlSCollection3[A, B, C] = new SqlSCollection3(a, b, c)
  def from[A: Schema, B: Schema, C: Schema, D: Schema](
    a: SCollection[A],
    b: SCollection[B],
    c: SCollection[C],
    d: SCollection[D]
  ): SqlSCollection4[A, B, C, D] = new SqlSCollection4(a, b, c, d)
  def from[A: Schema, B: Schema, C: Schema, D: Schema, E: Schema](
    a: SCollection[A],
    b: SCollection[B],
    c: SCollection[C],
    d: SCollection[D],
    e: SCollection[E]
  ): SqlSCollection5[A, B, C, D, E] = new SqlSCollection5(a, b, c, d, e)
  def from[A: Schema, B: Schema, C: Schema, D: Schema, E: Schema, F: Schema](
    a: SCollection[A],
    b: SCollection[B],
    c: SCollection[C],
    d: SCollection[D],
    e: SCollection[E],
    f: SCollection[F]
  ): SqlSCollection6[A, B, C, D, E, F] = new SqlSCollection6(a, b, c, d, e, f)
  def from[A: Schema, B: Schema, C: Schema, D: Schema, E: Schema, F: Schema, G: Schema](
    a: SCollection[A],
    b: SCollection[B],
    c: SCollection[C],
    d: SCollection[D],
    e: SCollection[E],
    f: SCollection[F],
    g: SCollection[G]
  ): SqlSCollection7[A, B, C, D, E, F, G] = new SqlSCollection7(a, b, c, d, e, f, g)
  def from[A: Schema, B: Schema, C: Schema, D: Schema, E: Schema, F: Schema, G: Schema, H: Schema](
    a: SCollection[A],
    b: SCollection[B],
    c: SCollection[C],
    d: SCollection[D],
    e: SCollection[E],
    f: SCollection[F],
    g: SCollection[G],
    h: SCollection[H]
  ): SqlSCollection8[A, B, C, D, E, F, G, H] = new SqlSCollection8(a, b, c, d, e, f, g, h)
  def from[
    A: Schema,
    B: Schema,
    C: Schema,
    D: Schema,
    E: Schema,
    F: Schema,
    G: Schema,
    H: Schema,
    I: Schema
  ](
    a: SCollection[A],
    b: SCollection[B],
    c: SCollection[C],
    d: SCollection[D],
    e: SCollection[E],
    f: SCollection[F],
    g: SCollection[G],
    h: SCollection[H],
    i: SCollection[I]
  ): SqlSCollection9[A, B, C, D, E, F, G, H, I] = new SqlSCollection9(a, b, c, d, e, f, g, h, i)
  def from[
    A: Schema,
    B: Schema,
    C: Schema,
    D: Schema,
    E: Schema,
    F: Schema,
    G: Schema,
    H: Schema,
    I: Schema,
    J: Schema
  ](
    a: SCollection[A],
    b: SCollection[B],
    c: SCollection[C],
    d: SCollection[D],
    e: SCollection[E],
    f: SCollection[F],
    g: SCollection[G],
    h: SCollection[H],
    i: SCollection[I],
    j: SCollection[J]
  ): SqlSCollection10[A, B, C, D, E, F, G, H, I, J] =
    new SqlSCollection10(a, b, c, d, e, f, g, h, i, j)
}
