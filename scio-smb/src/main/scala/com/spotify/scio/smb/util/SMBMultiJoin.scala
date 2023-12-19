/*
 * Copyright 2021 Spotify AB.
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

// generated with smb-multijoin.py
package com.spotify.scio.smb.util
import com.spotify.scio.ScioContext
import com.spotify.scio.coders.Coder
import com.spotify.scio.testing.TestDataManager
import com.spotify.scio.values._
import org.apache.beam.sdk.extensions.smb.{SortedBucketIO, SortedBucketIOUtil, TargetParallelism}
import org.apache.beam.sdk.transforms.join.{CoGbkResult, CoGroupByKey, KeyedPCollectionTuple}
import org.apache.beam.sdk.values.KV
import com.spotify.scio.smb.SortMergeTransform
import org.typelevel.scalaccompat.annotation.nowarn

import scala.jdk.CollectionConverters._
final class SMBMultiJoin(private val self: ScioContext) {
	def sortMergeCoGroup[KEY: Coder, A: Coder, B: Coder](keyClass: Class[KEY], a: SortedBucketIO.Read[A], b: SortedBucketIO.Read[B], targetParallelism: TargetParallelism): SCollection[(KEY, (Iterable[A], Iterable[B]))] = self.requireNotClosed {
		val tfName = self.tfName
		val keyed = if (self.isTest) {
			testCoGroup[KEY](a, b)
		} else {
			val transform = SortedBucketIO
				.read(keyClass)
				.of(a, b)
				.withTargetParallelism(targetParallelism)
			self.wrap(self.pipeline.apply(s"SMB CoGroupByKey@$tfName", transform))
		}
		val tupleTagA = a.getTupleTag
		val tupleTagB = b.getTupleTag
		keyed
			.withName(tfName)
			.map { kv =>
				val result = kv.getValue
				(
					kv.getKey(),
					(
						result.getAll(tupleTagA).asScala,
						result.getAll(tupleTagB).asScala
					)
				)
			}
	}

	def sortMergeCoGroup[KEY: Coder, A: Coder, B: Coder](keyClass: Class[KEY], a: SortedBucketIO.Read[A], b: SortedBucketIO.Read[B]): SCollection[(KEY, (Iterable[A], Iterable[B]))] = {
		sortMergeCoGroup(keyClass, a, b, TargetParallelism.auto())
	}

	def sortMergeCoGroup[KEY: Coder, A: Coder, B: Coder, C: Coder](keyClass: Class[KEY], a: SortedBucketIO.Read[A], b: SortedBucketIO.Read[B], c: SortedBucketIO.Read[C], targetParallelism: TargetParallelism): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C]))] = self.requireNotClosed {
		val tfName = self.tfName
		val keyed = if (self.isTest) {
			testCoGroup[KEY](a, b, c)
		} else {
			val transform = SortedBucketIO
				.read(keyClass)
				.of(a, b, c)
				.withTargetParallelism(targetParallelism)
			self.wrap(self.pipeline.apply(s"SMB CoGroupByKey@$tfName", transform))
		}
		val tupleTagA = a.getTupleTag
		val tupleTagB = b.getTupleTag
		val tupleTagC = c.getTupleTag
		keyed
			.withName(tfName)
			.map { kv =>
				val result = kv.getValue
				(
					kv.getKey(),
					(
						result.getAll(tupleTagA).asScala,
						result.getAll(tupleTagB).asScala,
						result.getAll(tupleTagC).asScala
					)
				)
			}
	}

	def sortMergeCoGroup[KEY: Coder, A: Coder, B: Coder, C: Coder](keyClass: Class[KEY], a: SortedBucketIO.Read[A], b: SortedBucketIO.Read[B], c: SortedBucketIO.Read[C]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C]))] = {
		sortMergeCoGroup(keyClass, a, b, c, TargetParallelism.auto())
	}

	def sortMergeCoGroup[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder](keyClass: Class[KEY], a: SortedBucketIO.Read[A], b: SortedBucketIO.Read[B], c: SortedBucketIO.Read[C], d: SortedBucketIO.Read[D], targetParallelism: TargetParallelism): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D]))] = self.requireNotClosed {
		val tfName = self.tfName
		val keyed = if (self.isTest) {
			testCoGroup[KEY](a, b, c, d)
		} else {
			val transform = SortedBucketIO
				.read(keyClass)
				.of(a, b, c, d)
				.withTargetParallelism(targetParallelism)
			self.wrap(self.pipeline.apply(s"SMB CoGroupByKey@$tfName", transform))
		}
		val tupleTagA = a.getTupleTag
		val tupleTagB = b.getTupleTag
		val tupleTagC = c.getTupleTag
		val tupleTagD = d.getTupleTag
		keyed
			.withName(tfName)
			.map { kv =>
				val result = kv.getValue
				(
					kv.getKey(),
					(
						result.getAll(tupleTagA).asScala,
						result.getAll(tupleTagB).asScala,
						result.getAll(tupleTagC).asScala,
						result.getAll(tupleTagD).asScala
					)
				)
			}
	}

	def sortMergeCoGroup[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder](keyClass: Class[KEY], a: SortedBucketIO.Read[A], b: SortedBucketIO.Read[B], c: SortedBucketIO.Read[C], d: SortedBucketIO.Read[D]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D]))] = {
		sortMergeCoGroup(keyClass, a, b, c, d, TargetParallelism.auto())
	}

	def sortMergeCoGroup[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder](keyClass: Class[KEY], a: SortedBucketIO.Read[A], b: SortedBucketIO.Read[B], c: SortedBucketIO.Read[C], d: SortedBucketIO.Read[D], e: SortedBucketIO.Read[E], targetParallelism: TargetParallelism): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E]))] = self.requireNotClosed {
		val tfName = self.tfName
		val keyed = if (self.isTest) {
			testCoGroup[KEY](a, b, c, d, e)
		} else {
			val transform = SortedBucketIO
				.read(keyClass)
				.of(a, b, c, d, e)
				.withTargetParallelism(targetParallelism)
			self.wrap(self.pipeline.apply(s"SMB CoGroupByKey@$tfName", transform))
		}
		val tupleTagA = a.getTupleTag
		val tupleTagB = b.getTupleTag
		val tupleTagC = c.getTupleTag
		val tupleTagD = d.getTupleTag
		val tupleTagE = e.getTupleTag
		keyed
			.withName(tfName)
			.map { kv =>
				val result = kv.getValue
				(
					kv.getKey(),
					(
						result.getAll(tupleTagA).asScala,
						result.getAll(tupleTagB).asScala,
						result.getAll(tupleTagC).asScala,
						result.getAll(tupleTagD).asScala,
						result.getAll(tupleTagE).asScala
					)
				)
			}
	}

	def sortMergeCoGroup[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder](keyClass: Class[KEY], a: SortedBucketIO.Read[A], b: SortedBucketIO.Read[B], c: SortedBucketIO.Read[C], d: SortedBucketIO.Read[D], e: SortedBucketIO.Read[E]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E]))] = {
		sortMergeCoGroup(keyClass, a, b, c, d, e, TargetParallelism.auto())
	}

	def sortMergeCoGroup[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder](keyClass: Class[KEY], a: SortedBucketIO.Read[A], b: SortedBucketIO.Read[B], c: SortedBucketIO.Read[C], d: SortedBucketIO.Read[D], e: SortedBucketIO.Read[E], f: SortedBucketIO.Read[F], targetParallelism: TargetParallelism): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F]))] = self.requireNotClosed {
		val tfName = self.tfName
		val keyed = if (self.isTest) {
			testCoGroup[KEY](a, b, c, d, e, f)
		} else {
			val transform = SortedBucketIO
				.read(keyClass)
				.of(a, b, c, d, e, f)
				.withTargetParallelism(targetParallelism)
			self.wrap(self.pipeline.apply(s"SMB CoGroupByKey@$tfName", transform))
		}
		val tupleTagA = a.getTupleTag
		val tupleTagB = b.getTupleTag
		val tupleTagC = c.getTupleTag
		val tupleTagD = d.getTupleTag
		val tupleTagE = e.getTupleTag
		val tupleTagF = f.getTupleTag
		keyed
			.withName(tfName)
			.map { kv =>
				val result = kv.getValue
				(
					kv.getKey(),
					(
						result.getAll(tupleTagA).asScala,
						result.getAll(tupleTagB).asScala,
						result.getAll(tupleTagC).asScala,
						result.getAll(tupleTagD).asScala,
						result.getAll(tupleTagE).asScala,
						result.getAll(tupleTagF).asScala
					)
				)
			}
	}

	def sortMergeCoGroup[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder](keyClass: Class[KEY], a: SortedBucketIO.Read[A], b: SortedBucketIO.Read[B], c: SortedBucketIO.Read[C], d: SortedBucketIO.Read[D], e: SortedBucketIO.Read[E], f: SortedBucketIO.Read[F]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F]))] = {
		sortMergeCoGroup(keyClass, a, b, c, d, e, f, TargetParallelism.auto())
	}

	def sortMergeCoGroup[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder](keyClass: Class[KEY], a: SortedBucketIO.Read[A], b: SortedBucketIO.Read[B], c: SortedBucketIO.Read[C], d: SortedBucketIO.Read[D], e: SortedBucketIO.Read[E], f: SortedBucketIO.Read[F], g: SortedBucketIO.Read[G], targetParallelism: TargetParallelism): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G]))] = self.requireNotClosed {
		val tfName = self.tfName
		val keyed = if (self.isTest) {
			testCoGroup[KEY](a, b, c, d, e, f, g)
		} else {
			val transform = SortedBucketIO
				.read(keyClass)
				.of(a, b, c, d, e, f, g)
				.withTargetParallelism(targetParallelism)
			self.wrap(self.pipeline.apply(s"SMB CoGroupByKey@$tfName", transform))
		}
		val tupleTagA = a.getTupleTag
		val tupleTagB = b.getTupleTag
		val tupleTagC = c.getTupleTag
		val tupleTagD = d.getTupleTag
		val tupleTagE = e.getTupleTag
		val tupleTagF = f.getTupleTag
		val tupleTagG = g.getTupleTag
		keyed
			.withName(tfName)
			.map { kv =>
				val result = kv.getValue
				(
					kv.getKey(),
					(
						result.getAll(tupleTagA).asScala,
						result.getAll(tupleTagB).asScala,
						result.getAll(tupleTagC).asScala,
						result.getAll(tupleTagD).asScala,
						result.getAll(tupleTagE).asScala,
						result.getAll(tupleTagF).asScala,
						result.getAll(tupleTagG).asScala
					)
				)
			}
	}

	def sortMergeCoGroup[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder](keyClass: Class[KEY], a: SortedBucketIO.Read[A], b: SortedBucketIO.Read[B], c: SortedBucketIO.Read[C], d: SortedBucketIO.Read[D], e: SortedBucketIO.Read[E], f: SortedBucketIO.Read[F], g: SortedBucketIO.Read[G]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G]))] = {
		sortMergeCoGroup(keyClass, a, b, c, d, e, f, g, TargetParallelism.auto())
	}

	def sortMergeCoGroup[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder](keyClass: Class[KEY], a: SortedBucketIO.Read[A], b: SortedBucketIO.Read[B], c: SortedBucketIO.Read[C], d: SortedBucketIO.Read[D], e: SortedBucketIO.Read[E], f: SortedBucketIO.Read[F], g: SortedBucketIO.Read[G], h: SortedBucketIO.Read[H], targetParallelism: TargetParallelism): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H]))] = self.requireNotClosed {
		val tfName = self.tfName
		val keyed = if (self.isTest) {
			testCoGroup[KEY](a, b, c, d, e, f, g, h)
		} else {
			val transform = SortedBucketIO
				.read(keyClass)
				.of(a, b, c, d, e, f, g, h)
				.withTargetParallelism(targetParallelism)
			self.wrap(self.pipeline.apply(s"SMB CoGroupByKey@$tfName", transform))
		}
		val tupleTagA = a.getTupleTag
		val tupleTagB = b.getTupleTag
		val tupleTagC = c.getTupleTag
		val tupleTagD = d.getTupleTag
		val tupleTagE = e.getTupleTag
		val tupleTagF = f.getTupleTag
		val tupleTagG = g.getTupleTag
		val tupleTagH = h.getTupleTag
		keyed
			.withName(tfName)
			.map { kv =>
				val result = kv.getValue
				(
					kv.getKey(),
					(
						result.getAll(tupleTagA).asScala,
						result.getAll(tupleTagB).asScala,
						result.getAll(tupleTagC).asScala,
						result.getAll(tupleTagD).asScala,
						result.getAll(tupleTagE).asScala,
						result.getAll(tupleTagF).asScala,
						result.getAll(tupleTagG).asScala,
						result.getAll(tupleTagH).asScala
					)
				)
			}
	}

	def sortMergeCoGroup[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder](keyClass: Class[KEY], a: SortedBucketIO.Read[A], b: SortedBucketIO.Read[B], c: SortedBucketIO.Read[C], d: SortedBucketIO.Read[D], e: SortedBucketIO.Read[E], f: SortedBucketIO.Read[F], g: SortedBucketIO.Read[G], h: SortedBucketIO.Read[H]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H]))] = {
		sortMergeCoGroup(keyClass, a, b, c, d, e, f, g, h, TargetParallelism.auto())
	}

	def sortMergeCoGroup[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder](keyClass: Class[KEY], a: SortedBucketIO.Read[A], b: SortedBucketIO.Read[B], c: SortedBucketIO.Read[C], d: SortedBucketIO.Read[D], e: SortedBucketIO.Read[E], f: SortedBucketIO.Read[F], g: SortedBucketIO.Read[G], h: SortedBucketIO.Read[H], i: SortedBucketIO.Read[I], targetParallelism: TargetParallelism): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I]))] = self.requireNotClosed {
		val tfName = self.tfName
		val keyed = if (self.isTest) {
			testCoGroup[KEY](a, b, c, d, e, f, g, h, i)
		} else {
			val transform = SortedBucketIO
				.read(keyClass)
				.of(a, b, c, d, e, f, g, h, i)
				.withTargetParallelism(targetParallelism)
			self.wrap(self.pipeline.apply(s"SMB CoGroupByKey@$tfName", transform))
		}
		val tupleTagA = a.getTupleTag
		val tupleTagB = b.getTupleTag
		val tupleTagC = c.getTupleTag
		val tupleTagD = d.getTupleTag
		val tupleTagE = e.getTupleTag
		val tupleTagF = f.getTupleTag
		val tupleTagG = g.getTupleTag
		val tupleTagH = h.getTupleTag
		val tupleTagI = i.getTupleTag
		keyed
			.withName(tfName)
			.map { kv =>
				val result = kv.getValue
				(
					kv.getKey(),
					(
						result.getAll(tupleTagA).asScala,
						result.getAll(tupleTagB).asScala,
						result.getAll(tupleTagC).asScala,
						result.getAll(tupleTagD).asScala,
						result.getAll(tupleTagE).asScala,
						result.getAll(tupleTagF).asScala,
						result.getAll(tupleTagG).asScala,
						result.getAll(tupleTagH).asScala,
						result.getAll(tupleTagI).asScala
					)
				)
			}
	}

	def sortMergeCoGroup[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder](keyClass: Class[KEY], a: SortedBucketIO.Read[A], b: SortedBucketIO.Read[B], c: SortedBucketIO.Read[C], d: SortedBucketIO.Read[D], e: SortedBucketIO.Read[E], f: SortedBucketIO.Read[F], g: SortedBucketIO.Read[G], h: SortedBucketIO.Read[H], i: SortedBucketIO.Read[I]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I]))] = {
		sortMergeCoGroup(keyClass, a, b, c, d, e, f, g, h, i, TargetParallelism.auto())
	}

	def sortMergeCoGroup[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder](keyClass: Class[KEY], a: SortedBucketIO.Read[A], b: SortedBucketIO.Read[B], c: SortedBucketIO.Read[C], d: SortedBucketIO.Read[D], e: SortedBucketIO.Read[E], f: SortedBucketIO.Read[F], g: SortedBucketIO.Read[G], h: SortedBucketIO.Read[H], i: SortedBucketIO.Read[I], j: SortedBucketIO.Read[J], targetParallelism: TargetParallelism): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J]))] = self.requireNotClosed {
		val tfName = self.tfName
		val keyed = if (self.isTest) {
			testCoGroup[KEY](a, b, c, d, e, f, g, h, i, j)
		} else {
			val transform = SortedBucketIO
				.read(keyClass)
				.of(a, b, c, d, e, f, g, h, i, j)
				.withTargetParallelism(targetParallelism)
			self.wrap(self.pipeline.apply(s"SMB CoGroupByKey@$tfName", transform))
		}
		val tupleTagA = a.getTupleTag
		val tupleTagB = b.getTupleTag
		val tupleTagC = c.getTupleTag
		val tupleTagD = d.getTupleTag
		val tupleTagE = e.getTupleTag
		val tupleTagF = f.getTupleTag
		val tupleTagG = g.getTupleTag
		val tupleTagH = h.getTupleTag
		val tupleTagI = i.getTupleTag
		val tupleTagJ = j.getTupleTag
		keyed
			.withName(tfName)
			.map { kv =>
				val result = kv.getValue
				(
					kv.getKey(),
					(
						result.getAll(tupleTagA).asScala,
						result.getAll(tupleTagB).asScala,
						result.getAll(tupleTagC).asScala,
						result.getAll(tupleTagD).asScala,
						result.getAll(tupleTagE).asScala,
						result.getAll(tupleTagF).asScala,
						result.getAll(tupleTagG).asScala,
						result.getAll(tupleTagH).asScala,
						result.getAll(tupleTagI).asScala,
						result.getAll(tupleTagJ).asScala
					)
				)
			}
	}

	def sortMergeCoGroup[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder](keyClass: Class[KEY], a: SortedBucketIO.Read[A], b: SortedBucketIO.Read[B], c: SortedBucketIO.Read[C], d: SortedBucketIO.Read[D], e: SortedBucketIO.Read[E], f: SortedBucketIO.Read[F], g: SortedBucketIO.Read[G], h: SortedBucketIO.Read[H], i: SortedBucketIO.Read[I], j: SortedBucketIO.Read[J]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J]))] = {
		sortMergeCoGroup(keyClass, a, b, c, d, e, f, g, h, i, j, TargetParallelism.auto())
	}

	def sortMergeCoGroup[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder](keyClass: Class[KEY], a: SortedBucketIO.Read[A], b: SortedBucketIO.Read[B], c: SortedBucketIO.Read[C], d: SortedBucketIO.Read[D], e: SortedBucketIO.Read[E], f: SortedBucketIO.Read[F], g: SortedBucketIO.Read[G], h: SortedBucketIO.Read[H], i: SortedBucketIO.Read[I], j: SortedBucketIO.Read[J], k: SortedBucketIO.Read[K], targetParallelism: TargetParallelism): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J], Iterable[K]))] = self.requireNotClosed {
		val tfName = self.tfName
		val keyed = if (self.isTest) {
			testCoGroup[KEY](a, b, c, d, e, f, g, h, i, j, k)
		} else {
			val transform = SortedBucketIO
				.read(keyClass)
				.of(a, b, c, d, e, f, g, h, i, j, k)
				.withTargetParallelism(targetParallelism)
			self.wrap(self.pipeline.apply(s"SMB CoGroupByKey@$tfName", transform))
		}
		val tupleTagA = a.getTupleTag
		val tupleTagB = b.getTupleTag
		val tupleTagC = c.getTupleTag
		val tupleTagD = d.getTupleTag
		val tupleTagE = e.getTupleTag
		val tupleTagF = f.getTupleTag
		val tupleTagG = g.getTupleTag
		val tupleTagH = h.getTupleTag
		val tupleTagI = i.getTupleTag
		val tupleTagJ = j.getTupleTag
		val tupleTagK = k.getTupleTag
		keyed
			.withName(tfName)
			.map { kv =>
				val result = kv.getValue
				(
					kv.getKey(),
					(
						result.getAll(tupleTagA).asScala,
						result.getAll(tupleTagB).asScala,
						result.getAll(tupleTagC).asScala,
						result.getAll(tupleTagD).asScala,
						result.getAll(tupleTagE).asScala,
						result.getAll(tupleTagF).asScala,
						result.getAll(tupleTagG).asScala,
						result.getAll(tupleTagH).asScala,
						result.getAll(tupleTagI).asScala,
						result.getAll(tupleTagJ).asScala,
						result.getAll(tupleTagK).asScala
					)
				)
			}
	}

	def sortMergeCoGroup[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder](keyClass: Class[KEY], a: SortedBucketIO.Read[A], b: SortedBucketIO.Read[B], c: SortedBucketIO.Read[C], d: SortedBucketIO.Read[D], e: SortedBucketIO.Read[E], f: SortedBucketIO.Read[F], g: SortedBucketIO.Read[G], h: SortedBucketIO.Read[H], i: SortedBucketIO.Read[I], j: SortedBucketIO.Read[J], k: SortedBucketIO.Read[K]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J], Iterable[K]))] = {
		sortMergeCoGroup(keyClass, a, b, c, d, e, f, g, h, i, j, k, TargetParallelism.auto())
	}

	def sortMergeCoGroup[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder](keyClass: Class[KEY], a: SortedBucketIO.Read[A], b: SortedBucketIO.Read[B], c: SortedBucketIO.Read[C], d: SortedBucketIO.Read[D], e: SortedBucketIO.Read[E], f: SortedBucketIO.Read[F], g: SortedBucketIO.Read[G], h: SortedBucketIO.Read[H], i: SortedBucketIO.Read[I], j: SortedBucketIO.Read[J], k: SortedBucketIO.Read[K], l: SortedBucketIO.Read[L], targetParallelism: TargetParallelism): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J], Iterable[K], Iterable[L]))] = self.requireNotClosed {
		val tfName = self.tfName
		val keyed = if (self.isTest) {
			testCoGroup[KEY](a, b, c, d, e, f, g, h, i, j, k, l)
		} else {
			val transform = SortedBucketIO
				.read(keyClass)
				.of(a, b, c, d, e, f, g, h, i, j, k, l)
				.withTargetParallelism(targetParallelism)
			self.wrap(self.pipeline.apply(s"SMB CoGroupByKey@$tfName", transform))
		}
		val tupleTagA = a.getTupleTag
		val tupleTagB = b.getTupleTag
		val tupleTagC = c.getTupleTag
		val tupleTagD = d.getTupleTag
		val tupleTagE = e.getTupleTag
		val tupleTagF = f.getTupleTag
		val tupleTagG = g.getTupleTag
		val tupleTagH = h.getTupleTag
		val tupleTagI = i.getTupleTag
		val tupleTagJ = j.getTupleTag
		val tupleTagK = k.getTupleTag
		val tupleTagL = l.getTupleTag
		keyed
			.withName(tfName)
			.map { kv =>
				val result = kv.getValue
				(
					kv.getKey(),
					(
						result.getAll(tupleTagA).asScala,
						result.getAll(tupleTagB).asScala,
						result.getAll(tupleTagC).asScala,
						result.getAll(tupleTagD).asScala,
						result.getAll(tupleTagE).asScala,
						result.getAll(tupleTagF).asScala,
						result.getAll(tupleTagG).asScala,
						result.getAll(tupleTagH).asScala,
						result.getAll(tupleTagI).asScala,
						result.getAll(tupleTagJ).asScala,
						result.getAll(tupleTagK).asScala,
						result.getAll(tupleTagL).asScala
					)
				)
			}
	}

	def sortMergeCoGroup[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder](keyClass: Class[KEY], a: SortedBucketIO.Read[A], b: SortedBucketIO.Read[B], c: SortedBucketIO.Read[C], d: SortedBucketIO.Read[D], e: SortedBucketIO.Read[E], f: SortedBucketIO.Read[F], g: SortedBucketIO.Read[G], h: SortedBucketIO.Read[H], i: SortedBucketIO.Read[I], j: SortedBucketIO.Read[J], k: SortedBucketIO.Read[K], l: SortedBucketIO.Read[L]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J], Iterable[K], Iterable[L]))] = {
		sortMergeCoGroup(keyClass, a, b, c, d, e, f, g, h, i, j, k, l, TargetParallelism.auto())
	}

	def sortMergeCoGroup[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder](keyClass: Class[KEY], a: SortedBucketIO.Read[A], b: SortedBucketIO.Read[B], c: SortedBucketIO.Read[C], d: SortedBucketIO.Read[D], e: SortedBucketIO.Read[E], f: SortedBucketIO.Read[F], g: SortedBucketIO.Read[G], h: SortedBucketIO.Read[H], i: SortedBucketIO.Read[I], j: SortedBucketIO.Read[J], k: SortedBucketIO.Read[K], l: SortedBucketIO.Read[L], m: SortedBucketIO.Read[M], targetParallelism: TargetParallelism): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J], Iterable[K], Iterable[L], Iterable[M]))] = self.requireNotClosed {
		val tfName = self.tfName
		val keyed = if (self.isTest) {
			testCoGroup[KEY](a, b, c, d, e, f, g, h, i, j, k, l, m)
		} else {
			val transform = SortedBucketIO
				.read(keyClass)
				.of(a, b, c, d, e, f, g, h, i, j, k, l, m)
				.withTargetParallelism(targetParallelism)
			self.wrap(self.pipeline.apply(s"SMB CoGroupByKey@$tfName", transform))
		}
		val tupleTagA = a.getTupleTag
		val tupleTagB = b.getTupleTag
		val tupleTagC = c.getTupleTag
		val tupleTagD = d.getTupleTag
		val tupleTagE = e.getTupleTag
		val tupleTagF = f.getTupleTag
		val tupleTagG = g.getTupleTag
		val tupleTagH = h.getTupleTag
		val tupleTagI = i.getTupleTag
		val tupleTagJ = j.getTupleTag
		val tupleTagK = k.getTupleTag
		val tupleTagL = l.getTupleTag
		val tupleTagM = m.getTupleTag
		keyed
			.withName(tfName)
			.map { kv =>
				val result = kv.getValue
				(
					kv.getKey(),
					(
						result.getAll(tupleTagA).asScala,
						result.getAll(tupleTagB).asScala,
						result.getAll(tupleTagC).asScala,
						result.getAll(tupleTagD).asScala,
						result.getAll(tupleTagE).asScala,
						result.getAll(tupleTagF).asScala,
						result.getAll(tupleTagG).asScala,
						result.getAll(tupleTagH).asScala,
						result.getAll(tupleTagI).asScala,
						result.getAll(tupleTagJ).asScala,
						result.getAll(tupleTagK).asScala,
						result.getAll(tupleTagL).asScala,
						result.getAll(tupleTagM).asScala
					)
				)
			}
	}

	def sortMergeCoGroup[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder](keyClass: Class[KEY], a: SortedBucketIO.Read[A], b: SortedBucketIO.Read[B], c: SortedBucketIO.Read[C], d: SortedBucketIO.Read[D], e: SortedBucketIO.Read[E], f: SortedBucketIO.Read[F], g: SortedBucketIO.Read[G], h: SortedBucketIO.Read[H], i: SortedBucketIO.Read[I], j: SortedBucketIO.Read[J], k: SortedBucketIO.Read[K], l: SortedBucketIO.Read[L], m: SortedBucketIO.Read[M]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J], Iterable[K], Iterable[L], Iterable[M]))] = {
		sortMergeCoGroup(keyClass, a, b, c, d, e, f, g, h, i, j, k, l, m, TargetParallelism.auto())
	}

	def sortMergeCoGroup[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder, N: Coder](keyClass: Class[KEY], a: SortedBucketIO.Read[A], b: SortedBucketIO.Read[B], c: SortedBucketIO.Read[C], d: SortedBucketIO.Read[D], e: SortedBucketIO.Read[E], f: SortedBucketIO.Read[F], g: SortedBucketIO.Read[G], h: SortedBucketIO.Read[H], i: SortedBucketIO.Read[I], j: SortedBucketIO.Read[J], k: SortedBucketIO.Read[K], l: SortedBucketIO.Read[L], m: SortedBucketIO.Read[M], n: SortedBucketIO.Read[N], targetParallelism: TargetParallelism): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J], Iterable[K], Iterable[L], Iterable[M], Iterable[N]))] = self.requireNotClosed {
		val tfName = self.tfName
		val keyed = if (self.isTest) {
			testCoGroup[KEY](a, b, c, d, e, f, g, h, i, j, k, l, m, n)
		} else {
			val transform = SortedBucketIO
				.read(keyClass)
				.of(a, b, c, d, e, f, g, h, i, j, k, l, m, n)
				.withTargetParallelism(targetParallelism)
			self.wrap(self.pipeline.apply(s"SMB CoGroupByKey@$tfName", transform))
		}
		val tupleTagA = a.getTupleTag
		val tupleTagB = b.getTupleTag
		val tupleTagC = c.getTupleTag
		val tupleTagD = d.getTupleTag
		val tupleTagE = e.getTupleTag
		val tupleTagF = f.getTupleTag
		val tupleTagG = g.getTupleTag
		val tupleTagH = h.getTupleTag
		val tupleTagI = i.getTupleTag
		val tupleTagJ = j.getTupleTag
		val tupleTagK = k.getTupleTag
		val tupleTagL = l.getTupleTag
		val tupleTagM = m.getTupleTag
		val tupleTagN = n.getTupleTag
		keyed
			.withName(tfName)
			.map { kv =>
				val result = kv.getValue
				(
					kv.getKey(),
					(
						result.getAll(tupleTagA).asScala,
						result.getAll(tupleTagB).asScala,
						result.getAll(tupleTagC).asScala,
						result.getAll(tupleTagD).asScala,
						result.getAll(tupleTagE).asScala,
						result.getAll(tupleTagF).asScala,
						result.getAll(tupleTagG).asScala,
						result.getAll(tupleTagH).asScala,
						result.getAll(tupleTagI).asScala,
						result.getAll(tupleTagJ).asScala,
						result.getAll(tupleTagK).asScala,
						result.getAll(tupleTagL).asScala,
						result.getAll(tupleTagM).asScala,
						result.getAll(tupleTagN).asScala
					)
				)
			}
	}

	def sortMergeCoGroup[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder, N: Coder](keyClass: Class[KEY], a: SortedBucketIO.Read[A], b: SortedBucketIO.Read[B], c: SortedBucketIO.Read[C], d: SortedBucketIO.Read[D], e: SortedBucketIO.Read[E], f: SortedBucketIO.Read[F], g: SortedBucketIO.Read[G], h: SortedBucketIO.Read[H], i: SortedBucketIO.Read[I], j: SortedBucketIO.Read[J], k: SortedBucketIO.Read[K], l: SortedBucketIO.Read[L], m: SortedBucketIO.Read[M], n: SortedBucketIO.Read[N]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J], Iterable[K], Iterable[L], Iterable[M], Iterable[N]))] = {
		sortMergeCoGroup(keyClass, a, b, c, d, e, f, g, h, i, j, k, l, m, n, TargetParallelism.auto())
	}

	def sortMergeCoGroup[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder, N: Coder, O: Coder](keyClass: Class[KEY], a: SortedBucketIO.Read[A], b: SortedBucketIO.Read[B], c: SortedBucketIO.Read[C], d: SortedBucketIO.Read[D], e: SortedBucketIO.Read[E], f: SortedBucketIO.Read[F], g: SortedBucketIO.Read[G], h: SortedBucketIO.Read[H], i: SortedBucketIO.Read[I], j: SortedBucketIO.Read[J], k: SortedBucketIO.Read[K], l: SortedBucketIO.Read[L], m: SortedBucketIO.Read[M], n: SortedBucketIO.Read[N], o: SortedBucketIO.Read[O], targetParallelism: TargetParallelism): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J], Iterable[K], Iterable[L], Iterable[M], Iterable[N], Iterable[O]))] = self.requireNotClosed {
		val tfName = self.tfName
		val keyed = if (self.isTest) {
			testCoGroup[KEY](a, b, c, d, e, f, g, h, i, j, k, l, m, n, o)
		} else {
			val transform = SortedBucketIO
				.read(keyClass)
				.of(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o)
				.withTargetParallelism(targetParallelism)
			self.wrap(self.pipeline.apply(s"SMB CoGroupByKey@$tfName", transform))
		}
		val tupleTagA = a.getTupleTag
		val tupleTagB = b.getTupleTag
		val tupleTagC = c.getTupleTag
		val tupleTagD = d.getTupleTag
		val tupleTagE = e.getTupleTag
		val tupleTagF = f.getTupleTag
		val tupleTagG = g.getTupleTag
		val tupleTagH = h.getTupleTag
		val tupleTagI = i.getTupleTag
		val tupleTagJ = j.getTupleTag
		val tupleTagK = k.getTupleTag
		val tupleTagL = l.getTupleTag
		val tupleTagM = m.getTupleTag
		val tupleTagN = n.getTupleTag
		val tupleTagO = o.getTupleTag
		keyed
			.withName(tfName)
			.map { kv =>
				val result = kv.getValue
				(
					kv.getKey(),
					(
						result.getAll(tupleTagA).asScala,
						result.getAll(tupleTagB).asScala,
						result.getAll(tupleTagC).asScala,
						result.getAll(tupleTagD).asScala,
						result.getAll(tupleTagE).asScala,
						result.getAll(tupleTagF).asScala,
						result.getAll(tupleTagG).asScala,
						result.getAll(tupleTagH).asScala,
						result.getAll(tupleTagI).asScala,
						result.getAll(tupleTagJ).asScala,
						result.getAll(tupleTagK).asScala,
						result.getAll(tupleTagL).asScala,
						result.getAll(tupleTagM).asScala,
						result.getAll(tupleTagN).asScala,
						result.getAll(tupleTagO).asScala
					)
				)
			}
	}

	def sortMergeCoGroup[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder, N: Coder, O: Coder](keyClass: Class[KEY], a: SortedBucketIO.Read[A], b: SortedBucketIO.Read[B], c: SortedBucketIO.Read[C], d: SortedBucketIO.Read[D], e: SortedBucketIO.Read[E], f: SortedBucketIO.Read[F], g: SortedBucketIO.Read[G], h: SortedBucketIO.Read[H], i: SortedBucketIO.Read[I], j: SortedBucketIO.Read[J], k: SortedBucketIO.Read[K], l: SortedBucketIO.Read[L], m: SortedBucketIO.Read[M], n: SortedBucketIO.Read[N], o: SortedBucketIO.Read[O]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J], Iterable[K], Iterable[L], Iterable[M], Iterable[N], Iterable[O]))] = {
		sortMergeCoGroup(keyClass, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, TargetParallelism.auto())
	}

	def sortMergeCoGroup[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder, N: Coder, O: Coder, P: Coder](keyClass: Class[KEY], a: SortedBucketIO.Read[A], b: SortedBucketIO.Read[B], c: SortedBucketIO.Read[C], d: SortedBucketIO.Read[D], e: SortedBucketIO.Read[E], f: SortedBucketIO.Read[F], g: SortedBucketIO.Read[G], h: SortedBucketIO.Read[H], i: SortedBucketIO.Read[I], j: SortedBucketIO.Read[J], k: SortedBucketIO.Read[K], l: SortedBucketIO.Read[L], m: SortedBucketIO.Read[M], n: SortedBucketIO.Read[N], o: SortedBucketIO.Read[O], p: SortedBucketIO.Read[P], targetParallelism: TargetParallelism): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J], Iterable[K], Iterable[L], Iterable[M], Iterable[N], Iterable[O], Iterable[P]))] = self.requireNotClosed {
		val tfName = self.tfName
		val keyed = if (self.isTest) {
			testCoGroup[KEY](a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p)
		} else {
			val transform = SortedBucketIO
				.read(keyClass)
				.of(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p)
				.withTargetParallelism(targetParallelism)
			self.wrap(self.pipeline.apply(s"SMB CoGroupByKey@$tfName", transform))
		}
		val tupleTagA = a.getTupleTag
		val tupleTagB = b.getTupleTag
		val tupleTagC = c.getTupleTag
		val tupleTagD = d.getTupleTag
		val tupleTagE = e.getTupleTag
		val tupleTagF = f.getTupleTag
		val tupleTagG = g.getTupleTag
		val tupleTagH = h.getTupleTag
		val tupleTagI = i.getTupleTag
		val tupleTagJ = j.getTupleTag
		val tupleTagK = k.getTupleTag
		val tupleTagL = l.getTupleTag
		val tupleTagM = m.getTupleTag
		val tupleTagN = n.getTupleTag
		val tupleTagO = o.getTupleTag
		val tupleTagP = p.getTupleTag
		keyed
			.withName(tfName)
			.map { kv =>
				val result = kv.getValue
				(
					kv.getKey(),
					(
						result.getAll(tupleTagA).asScala,
						result.getAll(tupleTagB).asScala,
						result.getAll(tupleTagC).asScala,
						result.getAll(tupleTagD).asScala,
						result.getAll(tupleTagE).asScala,
						result.getAll(tupleTagF).asScala,
						result.getAll(tupleTagG).asScala,
						result.getAll(tupleTagH).asScala,
						result.getAll(tupleTagI).asScala,
						result.getAll(tupleTagJ).asScala,
						result.getAll(tupleTagK).asScala,
						result.getAll(tupleTagL).asScala,
						result.getAll(tupleTagM).asScala,
						result.getAll(tupleTagN).asScala,
						result.getAll(tupleTagO).asScala,
						result.getAll(tupleTagP).asScala
					)
				)
			}
	}

	def sortMergeCoGroup[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder, N: Coder, O: Coder, P: Coder](keyClass: Class[KEY], a: SortedBucketIO.Read[A], b: SortedBucketIO.Read[B], c: SortedBucketIO.Read[C], d: SortedBucketIO.Read[D], e: SortedBucketIO.Read[E], f: SortedBucketIO.Read[F], g: SortedBucketIO.Read[G], h: SortedBucketIO.Read[H], i: SortedBucketIO.Read[I], j: SortedBucketIO.Read[J], k: SortedBucketIO.Read[K], l: SortedBucketIO.Read[L], m: SortedBucketIO.Read[M], n: SortedBucketIO.Read[N], o: SortedBucketIO.Read[O], p: SortedBucketIO.Read[P]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J], Iterable[K], Iterable[L], Iterable[M], Iterable[N], Iterable[O], Iterable[P]))] = {
		sortMergeCoGroup(keyClass, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, TargetParallelism.auto())
	}

	def sortMergeCoGroup[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder, N: Coder, O: Coder, P: Coder, Q: Coder](keyClass: Class[KEY], a: SortedBucketIO.Read[A], b: SortedBucketIO.Read[B], c: SortedBucketIO.Read[C], d: SortedBucketIO.Read[D], e: SortedBucketIO.Read[E], f: SortedBucketIO.Read[F], g: SortedBucketIO.Read[G], h: SortedBucketIO.Read[H], i: SortedBucketIO.Read[I], j: SortedBucketIO.Read[J], k: SortedBucketIO.Read[K], l: SortedBucketIO.Read[L], m: SortedBucketIO.Read[M], n: SortedBucketIO.Read[N], o: SortedBucketIO.Read[O], p: SortedBucketIO.Read[P], q: SortedBucketIO.Read[Q], targetParallelism: TargetParallelism): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J], Iterable[K], Iterable[L], Iterable[M], Iterable[N], Iterable[O], Iterable[P], Iterable[Q]))] = self.requireNotClosed {
		val tfName = self.tfName
		val keyed = if (self.isTest) {
			testCoGroup[KEY](a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q)
		} else {
			val transform = SortedBucketIO
				.read(keyClass)
				.of(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q)
				.withTargetParallelism(targetParallelism)
			self.wrap(self.pipeline.apply(s"SMB CoGroupByKey@$tfName", transform))
		}
		val tupleTagA = a.getTupleTag
		val tupleTagB = b.getTupleTag
		val tupleTagC = c.getTupleTag
		val tupleTagD = d.getTupleTag
		val tupleTagE = e.getTupleTag
		val tupleTagF = f.getTupleTag
		val tupleTagG = g.getTupleTag
		val tupleTagH = h.getTupleTag
		val tupleTagI = i.getTupleTag
		val tupleTagJ = j.getTupleTag
		val tupleTagK = k.getTupleTag
		val tupleTagL = l.getTupleTag
		val tupleTagM = m.getTupleTag
		val tupleTagN = n.getTupleTag
		val tupleTagO = o.getTupleTag
		val tupleTagP = p.getTupleTag
		val tupleTagQ = q.getTupleTag
		keyed
			.withName(tfName)
			.map { kv =>
				val result = kv.getValue
				(
					kv.getKey(),
					(
						result.getAll(tupleTagA).asScala,
						result.getAll(tupleTagB).asScala,
						result.getAll(tupleTagC).asScala,
						result.getAll(tupleTagD).asScala,
						result.getAll(tupleTagE).asScala,
						result.getAll(tupleTagF).asScala,
						result.getAll(tupleTagG).asScala,
						result.getAll(tupleTagH).asScala,
						result.getAll(tupleTagI).asScala,
						result.getAll(tupleTagJ).asScala,
						result.getAll(tupleTagK).asScala,
						result.getAll(tupleTagL).asScala,
						result.getAll(tupleTagM).asScala,
						result.getAll(tupleTagN).asScala,
						result.getAll(tupleTagO).asScala,
						result.getAll(tupleTagP).asScala,
						result.getAll(tupleTagQ).asScala
					)
				)
			}
	}

	def sortMergeCoGroup[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder, N: Coder, O: Coder, P: Coder, Q: Coder](keyClass: Class[KEY], a: SortedBucketIO.Read[A], b: SortedBucketIO.Read[B], c: SortedBucketIO.Read[C], d: SortedBucketIO.Read[D], e: SortedBucketIO.Read[E], f: SortedBucketIO.Read[F], g: SortedBucketIO.Read[G], h: SortedBucketIO.Read[H], i: SortedBucketIO.Read[I], j: SortedBucketIO.Read[J], k: SortedBucketIO.Read[K], l: SortedBucketIO.Read[L], m: SortedBucketIO.Read[M], n: SortedBucketIO.Read[N], o: SortedBucketIO.Read[O], p: SortedBucketIO.Read[P], q: SortedBucketIO.Read[Q]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J], Iterable[K], Iterable[L], Iterable[M], Iterable[N], Iterable[O], Iterable[P], Iterable[Q]))] = {
		sortMergeCoGroup(keyClass, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, TargetParallelism.auto())
	}

	def sortMergeCoGroup[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder, N: Coder, O: Coder, P: Coder, Q: Coder, R: Coder](keyClass: Class[KEY], a: SortedBucketIO.Read[A], b: SortedBucketIO.Read[B], c: SortedBucketIO.Read[C], d: SortedBucketIO.Read[D], e: SortedBucketIO.Read[E], f: SortedBucketIO.Read[F], g: SortedBucketIO.Read[G], h: SortedBucketIO.Read[H], i: SortedBucketIO.Read[I], j: SortedBucketIO.Read[J], k: SortedBucketIO.Read[K], l: SortedBucketIO.Read[L], m: SortedBucketIO.Read[M], n: SortedBucketIO.Read[N], o: SortedBucketIO.Read[O], p: SortedBucketIO.Read[P], q: SortedBucketIO.Read[Q], r: SortedBucketIO.Read[R], targetParallelism: TargetParallelism): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J], Iterable[K], Iterable[L], Iterable[M], Iterable[N], Iterable[O], Iterable[P], Iterable[Q], Iterable[R]))] = self.requireNotClosed {
		val tfName = self.tfName
		val keyed = if (self.isTest) {
			testCoGroup[KEY](a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r)
		} else {
			val transform = SortedBucketIO
				.read(keyClass)
				.of(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r)
				.withTargetParallelism(targetParallelism)
			self.wrap(self.pipeline.apply(s"SMB CoGroupByKey@$tfName", transform))
		}
		val tupleTagA = a.getTupleTag
		val tupleTagB = b.getTupleTag
		val tupleTagC = c.getTupleTag
		val tupleTagD = d.getTupleTag
		val tupleTagE = e.getTupleTag
		val tupleTagF = f.getTupleTag
		val tupleTagG = g.getTupleTag
		val tupleTagH = h.getTupleTag
		val tupleTagI = i.getTupleTag
		val tupleTagJ = j.getTupleTag
		val tupleTagK = k.getTupleTag
		val tupleTagL = l.getTupleTag
		val tupleTagM = m.getTupleTag
		val tupleTagN = n.getTupleTag
		val tupleTagO = o.getTupleTag
		val tupleTagP = p.getTupleTag
		val tupleTagQ = q.getTupleTag
		val tupleTagR = r.getTupleTag
		keyed
			.withName(tfName)
			.map { kv =>
				val result = kv.getValue
				(
					kv.getKey(),
					(
						result.getAll(tupleTagA).asScala,
						result.getAll(tupleTagB).asScala,
						result.getAll(tupleTagC).asScala,
						result.getAll(tupleTagD).asScala,
						result.getAll(tupleTagE).asScala,
						result.getAll(tupleTagF).asScala,
						result.getAll(tupleTagG).asScala,
						result.getAll(tupleTagH).asScala,
						result.getAll(tupleTagI).asScala,
						result.getAll(tupleTagJ).asScala,
						result.getAll(tupleTagK).asScala,
						result.getAll(tupleTagL).asScala,
						result.getAll(tupleTagM).asScala,
						result.getAll(tupleTagN).asScala,
						result.getAll(tupleTagO).asScala,
						result.getAll(tupleTagP).asScala,
						result.getAll(tupleTagQ).asScala,
						result.getAll(tupleTagR).asScala
					)
				)
			}
	}

	def sortMergeCoGroup[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder, N: Coder, O: Coder, P: Coder, Q: Coder, R: Coder](keyClass: Class[KEY], a: SortedBucketIO.Read[A], b: SortedBucketIO.Read[B], c: SortedBucketIO.Read[C], d: SortedBucketIO.Read[D], e: SortedBucketIO.Read[E], f: SortedBucketIO.Read[F], g: SortedBucketIO.Read[G], h: SortedBucketIO.Read[H], i: SortedBucketIO.Read[I], j: SortedBucketIO.Read[J], k: SortedBucketIO.Read[K], l: SortedBucketIO.Read[L], m: SortedBucketIO.Read[M], n: SortedBucketIO.Read[N], o: SortedBucketIO.Read[O], p: SortedBucketIO.Read[P], q: SortedBucketIO.Read[Q], r: SortedBucketIO.Read[R]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J], Iterable[K], Iterable[L], Iterable[M], Iterable[N], Iterable[O], Iterable[P], Iterable[Q], Iterable[R]))] = {
		sortMergeCoGroup(keyClass, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, TargetParallelism.auto())
	}

	def sortMergeCoGroup[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder, N: Coder, O: Coder, P: Coder, Q: Coder, R: Coder, S: Coder](keyClass: Class[KEY], a: SortedBucketIO.Read[A], b: SortedBucketIO.Read[B], c: SortedBucketIO.Read[C], d: SortedBucketIO.Read[D], e: SortedBucketIO.Read[E], f: SortedBucketIO.Read[F], g: SortedBucketIO.Read[G], h: SortedBucketIO.Read[H], i: SortedBucketIO.Read[I], j: SortedBucketIO.Read[J], k: SortedBucketIO.Read[K], l: SortedBucketIO.Read[L], m: SortedBucketIO.Read[M], n: SortedBucketIO.Read[N], o: SortedBucketIO.Read[O], p: SortedBucketIO.Read[P], q: SortedBucketIO.Read[Q], r: SortedBucketIO.Read[R], s: SortedBucketIO.Read[S], targetParallelism: TargetParallelism): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J], Iterable[K], Iterable[L], Iterable[M], Iterable[N], Iterable[O], Iterable[P], Iterable[Q], Iterable[R], Iterable[S]))] = self.requireNotClosed {
		val tfName = self.tfName
		val keyed = if (self.isTest) {
			testCoGroup[KEY](a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s)
		} else {
			val transform = SortedBucketIO
				.read(keyClass)
				.of(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s)
				.withTargetParallelism(targetParallelism)
			self.wrap(self.pipeline.apply(s"SMB CoGroupByKey@$tfName", transform))
		}
		val tupleTagA = a.getTupleTag
		val tupleTagB = b.getTupleTag
		val tupleTagC = c.getTupleTag
		val tupleTagD = d.getTupleTag
		val tupleTagE = e.getTupleTag
		val tupleTagF = f.getTupleTag
		val tupleTagG = g.getTupleTag
		val tupleTagH = h.getTupleTag
		val tupleTagI = i.getTupleTag
		val tupleTagJ = j.getTupleTag
		val tupleTagK = k.getTupleTag
		val tupleTagL = l.getTupleTag
		val tupleTagM = m.getTupleTag
		val tupleTagN = n.getTupleTag
		val tupleTagO = o.getTupleTag
		val tupleTagP = p.getTupleTag
		val tupleTagQ = q.getTupleTag
		val tupleTagR = r.getTupleTag
		val tupleTagS = s.getTupleTag
		keyed
			.withName(tfName)
			.map { kv =>
				val result = kv.getValue
				(
					kv.getKey(),
					(
						result.getAll(tupleTagA).asScala,
						result.getAll(tupleTagB).asScala,
						result.getAll(tupleTagC).asScala,
						result.getAll(tupleTagD).asScala,
						result.getAll(tupleTagE).asScala,
						result.getAll(tupleTagF).asScala,
						result.getAll(tupleTagG).asScala,
						result.getAll(tupleTagH).asScala,
						result.getAll(tupleTagI).asScala,
						result.getAll(tupleTagJ).asScala,
						result.getAll(tupleTagK).asScala,
						result.getAll(tupleTagL).asScala,
						result.getAll(tupleTagM).asScala,
						result.getAll(tupleTagN).asScala,
						result.getAll(tupleTagO).asScala,
						result.getAll(tupleTagP).asScala,
						result.getAll(tupleTagQ).asScala,
						result.getAll(tupleTagR).asScala,
						result.getAll(tupleTagS).asScala
					)
				)
			}
	}

	def sortMergeCoGroup[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder, N: Coder, O: Coder, P: Coder, Q: Coder, R: Coder, S: Coder](keyClass: Class[KEY], a: SortedBucketIO.Read[A], b: SortedBucketIO.Read[B], c: SortedBucketIO.Read[C], d: SortedBucketIO.Read[D], e: SortedBucketIO.Read[E], f: SortedBucketIO.Read[F], g: SortedBucketIO.Read[G], h: SortedBucketIO.Read[H], i: SortedBucketIO.Read[I], j: SortedBucketIO.Read[J], k: SortedBucketIO.Read[K], l: SortedBucketIO.Read[L], m: SortedBucketIO.Read[M], n: SortedBucketIO.Read[N], o: SortedBucketIO.Read[O], p: SortedBucketIO.Read[P], q: SortedBucketIO.Read[Q], r: SortedBucketIO.Read[R], s: SortedBucketIO.Read[S]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J], Iterable[K], Iterable[L], Iterable[M], Iterable[N], Iterable[O], Iterable[P], Iterable[Q], Iterable[R], Iterable[S]))] = {
		sortMergeCoGroup(keyClass, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, TargetParallelism.auto())
	}

	def sortMergeCoGroup[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder, N: Coder, O: Coder, P: Coder, Q: Coder, R: Coder, S: Coder, T: Coder](keyClass: Class[KEY], a: SortedBucketIO.Read[A], b: SortedBucketIO.Read[B], c: SortedBucketIO.Read[C], d: SortedBucketIO.Read[D], e: SortedBucketIO.Read[E], f: SortedBucketIO.Read[F], g: SortedBucketIO.Read[G], h: SortedBucketIO.Read[H], i: SortedBucketIO.Read[I], j: SortedBucketIO.Read[J], k: SortedBucketIO.Read[K], l: SortedBucketIO.Read[L], m: SortedBucketIO.Read[M], n: SortedBucketIO.Read[N], o: SortedBucketIO.Read[O], p: SortedBucketIO.Read[P], q: SortedBucketIO.Read[Q], r: SortedBucketIO.Read[R], s: SortedBucketIO.Read[S], t: SortedBucketIO.Read[T], targetParallelism: TargetParallelism): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J], Iterable[K], Iterable[L], Iterable[M], Iterable[N], Iterable[O], Iterable[P], Iterable[Q], Iterable[R], Iterable[S], Iterable[T]))] = self.requireNotClosed {
		val tfName = self.tfName
		val keyed = if (self.isTest) {
			testCoGroup[KEY](a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t)
		} else {
			val transform = SortedBucketIO
				.read(keyClass)
				.of(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t)
				.withTargetParallelism(targetParallelism)
			self.wrap(self.pipeline.apply(s"SMB CoGroupByKey@$tfName", transform))
		}
		val tupleTagA = a.getTupleTag
		val tupleTagB = b.getTupleTag
		val tupleTagC = c.getTupleTag
		val tupleTagD = d.getTupleTag
		val tupleTagE = e.getTupleTag
		val tupleTagF = f.getTupleTag
		val tupleTagG = g.getTupleTag
		val tupleTagH = h.getTupleTag
		val tupleTagI = i.getTupleTag
		val tupleTagJ = j.getTupleTag
		val tupleTagK = k.getTupleTag
		val tupleTagL = l.getTupleTag
		val tupleTagM = m.getTupleTag
		val tupleTagN = n.getTupleTag
		val tupleTagO = o.getTupleTag
		val tupleTagP = p.getTupleTag
		val tupleTagQ = q.getTupleTag
		val tupleTagR = r.getTupleTag
		val tupleTagS = s.getTupleTag
		val tupleTagT = t.getTupleTag
		keyed
			.withName(tfName)
			.map { kv =>
				val result = kv.getValue
				(
					kv.getKey(),
					(
						result.getAll(tupleTagA).asScala,
						result.getAll(tupleTagB).asScala,
						result.getAll(tupleTagC).asScala,
						result.getAll(tupleTagD).asScala,
						result.getAll(tupleTagE).asScala,
						result.getAll(tupleTagF).asScala,
						result.getAll(tupleTagG).asScala,
						result.getAll(tupleTagH).asScala,
						result.getAll(tupleTagI).asScala,
						result.getAll(tupleTagJ).asScala,
						result.getAll(tupleTagK).asScala,
						result.getAll(tupleTagL).asScala,
						result.getAll(tupleTagM).asScala,
						result.getAll(tupleTagN).asScala,
						result.getAll(tupleTagO).asScala,
						result.getAll(tupleTagP).asScala,
						result.getAll(tupleTagQ).asScala,
						result.getAll(tupleTagR).asScala,
						result.getAll(tupleTagS).asScala,
						result.getAll(tupleTagT).asScala
					)
				)
			}
	}

	def sortMergeCoGroup[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder, N: Coder, O: Coder, P: Coder, Q: Coder, R: Coder, S: Coder, T: Coder](keyClass: Class[KEY], a: SortedBucketIO.Read[A], b: SortedBucketIO.Read[B], c: SortedBucketIO.Read[C], d: SortedBucketIO.Read[D], e: SortedBucketIO.Read[E], f: SortedBucketIO.Read[F], g: SortedBucketIO.Read[G], h: SortedBucketIO.Read[H], i: SortedBucketIO.Read[I], j: SortedBucketIO.Read[J], k: SortedBucketIO.Read[K], l: SortedBucketIO.Read[L], m: SortedBucketIO.Read[M], n: SortedBucketIO.Read[N], o: SortedBucketIO.Read[O], p: SortedBucketIO.Read[P], q: SortedBucketIO.Read[Q], r: SortedBucketIO.Read[R], s: SortedBucketIO.Read[S], t: SortedBucketIO.Read[T]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J], Iterable[K], Iterable[L], Iterable[M], Iterable[N], Iterable[O], Iterable[P], Iterable[Q], Iterable[R], Iterable[S], Iterable[T]))] = {
		sortMergeCoGroup(keyClass, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, TargetParallelism.auto())
	}

	def sortMergeCoGroup[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder, N: Coder, O: Coder, P: Coder, Q: Coder, R: Coder, S: Coder, T: Coder, U: Coder](keyClass: Class[KEY], a: SortedBucketIO.Read[A], b: SortedBucketIO.Read[B], c: SortedBucketIO.Read[C], d: SortedBucketIO.Read[D], e: SortedBucketIO.Read[E], f: SortedBucketIO.Read[F], g: SortedBucketIO.Read[G], h: SortedBucketIO.Read[H], i: SortedBucketIO.Read[I], j: SortedBucketIO.Read[J], k: SortedBucketIO.Read[K], l: SortedBucketIO.Read[L], m: SortedBucketIO.Read[M], n: SortedBucketIO.Read[N], o: SortedBucketIO.Read[O], p: SortedBucketIO.Read[P], q: SortedBucketIO.Read[Q], r: SortedBucketIO.Read[R], s: SortedBucketIO.Read[S], t: SortedBucketIO.Read[T], u: SortedBucketIO.Read[U], targetParallelism: TargetParallelism): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J], Iterable[K], Iterable[L], Iterable[M], Iterable[N], Iterable[O], Iterable[P], Iterable[Q], Iterable[R], Iterable[S], Iterable[T], Iterable[U]))] = self.requireNotClosed {
		val tfName = self.tfName
		val keyed = if (self.isTest) {
			testCoGroup[KEY](a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u)
		} else {
			val transform = SortedBucketIO
				.read(keyClass)
				.of(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u)
				.withTargetParallelism(targetParallelism)
			self.wrap(self.pipeline.apply(s"SMB CoGroupByKey@$tfName", transform))
		}
		val tupleTagA = a.getTupleTag
		val tupleTagB = b.getTupleTag
		val tupleTagC = c.getTupleTag
		val tupleTagD = d.getTupleTag
		val tupleTagE = e.getTupleTag
		val tupleTagF = f.getTupleTag
		val tupleTagG = g.getTupleTag
		val tupleTagH = h.getTupleTag
		val tupleTagI = i.getTupleTag
		val tupleTagJ = j.getTupleTag
		val tupleTagK = k.getTupleTag
		val tupleTagL = l.getTupleTag
		val tupleTagM = m.getTupleTag
		val tupleTagN = n.getTupleTag
		val tupleTagO = o.getTupleTag
		val tupleTagP = p.getTupleTag
		val tupleTagQ = q.getTupleTag
		val tupleTagR = r.getTupleTag
		val tupleTagS = s.getTupleTag
		val tupleTagT = t.getTupleTag
		val tupleTagU = u.getTupleTag
		keyed
			.withName(tfName)
			.map { kv =>
				val result = kv.getValue
				(
					kv.getKey(),
					(
						result.getAll(tupleTagA).asScala,
						result.getAll(tupleTagB).asScala,
						result.getAll(tupleTagC).asScala,
						result.getAll(tupleTagD).asScala,
						result.getAll(tupleTagE).asScala,
						result.getAll(tupleTagF).asScala,
						result.getAll(tupleTagG).asScala,
						result.getAll(tupleTagH).asScala,
						result.getAll(tupleTagI).asScala,
						result.getAll(tupleTagJ).asScala,
						result.getAll(tupleTagK).asScala,
						result.getAll(tupleTagL).asScala,
						result.getAll(tupleTagM).asScala,
						result.getAll(tupleTagN).asScala,
						result.getAll(tupleTagO).asScala,
						result.getAll(tupleTagP).asScala,
						result.getAll(tupleTagQ).asScala,
						result.getAll(tupleTagR).asScala,
						result.getAll(tupleTagS).asScala,
						result.getAll(tupleTagT).asScala,
						result.getAll(tupleTagU).asScala
					)
				)
			}
	}

	def sortMergeCoGroup[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder, N: Coder, O: Coder, P: Coder, Q: Coder, R: Coder, S: Coder, T: Coder, U: Coder](keyClass: Class[KEY], a: SortedBucketIO.Read[A], b: SortedBucketIO.Read[B], c: SortedBucketIO.Read[C], d: SortedBucketIO.Read[D], e: SortedBucketIO.Read[E], f: SortedBucketIO.Read[F], g: SortedBucketIO.Read[G], h: SortedBucketIO.Read[H], i: SortedBucketIO.Read[I], j: SortedBucketIO.Read[J], k: SortedBucketIO.Read[K], l: SortedBucketIO.Read[L], m: SortedBucketIO.Read[M], n: SortedBucketIO.Read[N], o: SortedBucketIO.Read[O], p: SortedBucketIO.Read[P], q: SortedBucketIO.Read[Q], r: SortedBucketIO.Read[R], s: SortedBucketIO.Read[S], t: SortedBucketIO.Read[T], u: SortedBucketIO.Read[U]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J], Iterable[K], Iterable[L], Iterable[M], Iterable[N], Iterable[O], Iterable[P], Iterable[Q], Iterable[R], Iterable[S], Iterable[T], Iterable[U]))] = {
		sortMergeCoGroup(keyClass, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, TargetParallelism.auto())
	}

	def sortMergeCoGroup[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder, N: Coder, O: Coder, P: Coder, Q: Coder, R: Coder, S: Coder, T: Coder, U: Coder, V: Coder](keyClass: Class[KEY], a: SortedBucketIO.Read[A], b: SortedBucketIO.Read[B], c: SortedBucketIO.Read[C], d: SortedBucketIO.Read[D], e: SortedBucketIO.Read[E], f: SortedBucketIO.Read[F], g: SortedBucketIO.Read[G], h: SortedBucketIO.Read[H], i: SortedBucketIO.Read[I], j: SortedBucketIO.Read[J], k: SortedBucketIO.Read[K], l: SortedBucketIO.Read[L], m: SortedBucketIO.Read[M], n: SortedBucketIO.Read[N], o: SortedBucketIO.Read[O], p: SortedBucketIO.Read[P], q: SortedBucketIO.Read[Q], r: SortedBucketIO.Read[R], s: SortedBucketIO.Read[S], t: SortedBucketIO.Read[T], u: SortedBucketIO.Read[U], v: SortedBucketIO.Read[V], targetParallelism: TargetParallelism): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J], Iterable[K], Iterable[L], Iterable[M], Iterable[N], Iterable[O], Iterable[P], Iterable[Q], Iterable[R], Iterable[S], Iterable[T], Iterable[U], Iterable[V]))] = self.requireNotClosed {
		val tfName = self.tfName
		val keyed = if (self.isTest) {
			testCoGroup[KEY](a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v)
		} else {
			val transform = SortedBucketIO
				.read(keyClass)
				.of(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v)
				.withTargetParallelism(targetParallelism)
			self.wrap(self.pipeline.apply(s"SMB CoGroupByKey@$tfName", transform))
		}
		val tupleTagA = a.getTupleTag
		val tupleTagB = b.getTupleTag
		val tupleTagC = c.getTupleTag
		val tupleTagD = d.getTupleTag
		val tupleTagE = e.getTupleTag
		val tupleTagF = f.getTupleTag
		val tupleTagG = g.getTupleTag
		val tupleTagH = h.getTupleTag
		val tupleTagI = i.getTupleTag
		val tupleTagJ = j.getTupleTag
		val tupleTagK = k.getTupleTag
		val tupleTagL = l.getTupleTag
		val tupleTagM = m.getTupleTag
		val tupleTagN = n.getTupleTag
		val tupleTagO = o.getTupleTag
		val tupleTagP = p.getTupleTag
		val tupleTagQ = q.getTupleTag
		val tupleTagR = r.getTupleTag
		val tupleTagS = s.getTupleTag
		val tupleTagT = t.getTupleTag
		val tupleTagU = u.getTupleTag
		val tupleTagV = v.getTupleTag
		keyed
			.withName(tfName)
			.map { kv =>
				val result = kv.getValue
				(
					kv.getKey(),
					(
						result.getAll(tupleTagA).asScala,
						result.getAll(tupleTagB).asScala,
						result.getAll(tupleTagC).asScala,
						result.getAll(tupleTagD).asScala,
						result.getAll(tupleTagE).asScala,
						result.getAll(tupleTagF).asScala,
						result.getAll(tupleTagG).asScala,
						result.getAll(tupleTagH).asScala,
						result.getAll(tupleTagI).asScala,
						result.getAll(tupleTagJ).asScala,
						result.getAll(tupleTagK).asScala,
						result.getAll(tupleTagL).asScala,
						result.getAll(tupleTagM).asScala,
						result.getAll(tupleTagN).asScala,
						result.getAll(tupleTagO).asScala,
						result.getAll(tupleTagP).asScala,
						result.getAll(tupleTagQ).asScala,
						result.getAll(tupleTagR).asScala,
						result.getAll(tupleTagS).asScala,
						result.getAll(tupleTagT).asScala,
						result.getAll(tupleTagU).asScala,
						result.getAll(tupleTagV).asScala
					)
				)
			}
	}

	def sortMergeCoGroup[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder, N: Coder, O: Coder, P: Coder, Q: Coder, R: Coder, S: Coder, T: Coder, U: Coder, V: Coder](keyClass: Class[KEY], a: SortedBucketIO.Read[A], b: SortedBucketIO.Read[B], c: SortedBucketIO.Read[C], d: SortedBucketIO.Read[D], e: SortedBucketIO.Read[E], f: SortedBucketIO.Read[F], g: SortedBucketIO.Read[G], h: SortedBucketIO.Read[H], i: SortedBucketIO.Read[I], j: SortedBucketIO.Read[J], k: SortedBucketIO.Read[K], l: SortedBucketIO.Read[L], m: SortedBucketIO.Read[M], n: SortedBucketIO.Read[N], o: SortedBucketIO.Read[O], p: SortedBucketIO.Read[P], q: SortedBucketIO.Read[Q], r: SortedBucketIO.Read[R], s: SortedBucketIO.Read[S], t: SortedBucketIO.Read[T], u: SortedBucketIO.Read[U], v: SortedBucketIO.Read[V]): SCollection[(KEY, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J], Iterable[K], Iterable[L], Iterable[M], Iterable[N], Iterable[O], Iterable[P], Iterable[Q], Iterable[R], Iterable[S], Iterable[T], Iterable[U], Iterable[V]))] = {
		sortMergeCoGroup(keyClass, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, TargetParallelism.auto())
	}

	def sortMergeTransform[KEY: Coder, A: Coder, B: Coder](keyClass: Class[KEY], a: SortedBucketIO.Read[A], b: SortedBucketIO.Read[B], targetParallelism: TargetParallelism): SortMergeTransform.ReadBuilder[KEY, KEY, Void, (Iterable[A], Iterable[B])] = self.requireNotClosed {
		val tupleTagA = a.getTupleTag
		val tupleTagB = b.getTupleTag
		val fromResult = { (result: CoGbkResult) =>
			(
				result.getAll(tupleTagA).asScala,
				result.getAll(tupleTagB).asScala
			)
		}
		if (self.isTest) {
			val result = testCoGroup[KEY](a, b)
			val keyed = result.map(kv => kv.getKey -> fromResult(kv.getValue))
			new SortMergeTransform.ReadBuilderTest(self, keyed)
		} else {
			val transform = SortedBucketIO
				.read(keyClass)
				.of(a, b)
				.withTargetParallelism(targetParallelism)
			new SortMergeTransform.ReadBuilderImpl(self, transform, fromResult)
		}
	}

	def sortMergeTransform[KEY: Coder, A: Coder, B: Coder](keyClass: Class[KEY], a: SortedBucketIO.Read[A], b: SortedBucketIO.Read[B]): SortMergeTransform.ReadBuilder[KEY, KEY, Void, (Iterable[A], Iterable[B])] = {
		sortMergeTransform(keyClass, a, b, TargetParallelism.auto())
	}

	def sortMergeTransform[KEY: Coder, A: Coder, B: Coder, C: Coder](keyClass: Class[KEY], a: SortedBucketIO.Read[A], b: SortedBucketIO.Read[B], c: SortedBucketIO.Read[C], targetParallelism: TargetParallelism): SortMergeTransform.ReadBuilder[KEY, KEY, Void, (Iterable[A], Iterable[B], Iterable[C])] = self.requireNotClosed {
		val tupleTagA = a.getTupleTag
		val tupleTagB = b.getTupleTag
		val tupleTagC = c.getTupleTag
		val fromResult = { (result: CoGbkResult) =>
			(
				result.getAll(tupleTagA).asScala,
				result.getAll(tupleTagB).asScala,
				result.getAll(tupleTagC).asScala
			)
		}
		if (self.isTest) {
			val result = testCoGroup[KEY](a, b, c)
			val keyed = result.map(kv => kv.getKey -> fromResult(kv.getValue))
			new SortMergeTransform.ReadBuilderTest(self, keyed)
		} else {
			val transform = SortedBucketIO
				.read(keyClass)
				.of(a, b, c)
				.withTargetParallelism(targetParallelism)
			new SortMergeTransform.ReadBuilderImpl(self, transform, fromResult)
		}
	}

	def sortMergeTransform[KEY: Coder, A: Coder, B: Coder, C: Coder](keyClass: Class[KEY], a: SortedBucketIO.Read[A], b: SortedBucketIO.Read[B], c: SortedBucketIO.Read[C]): SortMergeTransform.ReadBuilder[KEY, KEY, Void, (Iterable[A], Iterable[B], Iterable[C])] = {
		sortMergeTransform(keyClass, a, b, c, TargetParallelism.auto())
	}

	def sortMergeTransform[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder](keyClass: Class[KEY], a: SortedBucketIO.Read[A], b: SortedBucketIO.Read[B], c: SortedBucketIO.Read[C], d: SortedBucketIO.Read[D], targetParallelism: TargetParallelism): SortMergeTransform.ReadBuilder[KEY, KEY, Void, (Iterable[A], Iterable[B], Iterable[C], Iterable[D])] = self.requireNotClosed {
		val tupleTagA = a.getTupleTag
		val tupleTagB = b.getTupleTag
		val tupleTagC = c.getTupleTag
		val tupleTagD = d.getTupleTag
		val fromResult = { (result: CoGbkResult) =>
			(
				result.getAll(tupleTagA).asScala,
				result.getAll(tupleTagB).asScala,
				result.getAll(tupleTagC).asScala,
				result.getAll(tupleTagD).asScala
			)
		}
		if (self.isTest) {
			val result = testCoGroup[KEY](a, b, c, d)
			val keyed = result.map(kv => kv.getKey -> fromResult(kv.getValue))
			new SortMergeTransform.ReadBuilderTest(self, keyed)
		} else {
			val transform = SortedBucketIO
				.read(keyClass)
				.of(a, b, c, d)
				.withTargetParallelism(targetParallelism)
			new SortMergeTransform.ReadBuilderImpl(self, transform, fromResult)
		}
	}

	def sortMergeTransform[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder](keyClass: Class[KEY], a: SortedBucketIO.Read[A], b: SortedBucketIO.Read[B], c: SortedBucketIO.Read[C], d: SortedBucketIO.Read[D]): SortMergeTransform.ReadBuilder[KEY, KEY, Void, (Iterable[A], Iterable[B], Iterable[C], Iterable[D])] = {
		sortMergeTransform(keyClass, a, b, c, d, TargetParallelism.auto())
	}

	def sortMergeTransform[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder](keyClass: Class[KEY], a: SortedBucketIO.Read[A], b: SortedBucketIO.Read[B], c: SortedBucketIO.Read[C], d: SortedBucketIO.Read[D], e: SortedBucketIO.Read[E], targetParallelism: TargetParallelism): SortMergeTransform.ReadBuilder[KEY, KEY, Void, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E])] = self.requireNotClosed {
		val tupleTagA = a.getTupleTag
		val tupleTagB = b.getTupleTag
		val tupleTagC = c.getTupleTag
		val tupleTagD = d.getTupleTag
		val tupleTagE = e.getTupleTag
		val fromResult = { (result: CoGbkResult) =>
			(
				result.getAll(tupleTagA).asScala,
				result.getAll(tupleTagB).asScala,
				result.getAll(tupleTagC).asScala,
				result.getAll(tupleTagD).asScala,
				result.getAll(tupleTagE).asScala
			)
		}
		if (self.isTest) {
			val result = testCoGroup[KEY](a, b, c, d, e)
			val keyed = result.map(kv => kv.getKey -> fromResult(kv.getValue))
			new SortMergeTransform.ReadBuilderTest(self, keyed)
		} else {
			val transform = SortedBucketIO
				.read(keyClass)
				.of(a, b, c, d, e)
				.withTargetParallelism(targetParallelism)
			new SortMergeTransform.ReadBuilderImpl(self, transform, fromResult)
		}
	}

	def sortMergeTransform[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder](keyClass: Class[KEY], a: SortedBucketIO.Read[A], b: SortedBucketIO.Read[B], c: SortedBucketIO.Read[C], d: SortedBucketIO.Read[D], e: SortedBucketIO.Read[E]): SortMergeTransform.ReadBuilder[KEY, KEY, Void, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E])] = {
		sortMergeTransform(keyClass, a, b, c, d, e, TargetParallelism.auto())
	}

	def sortMergeTransform[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder](keyClass: Class[KEY], a: SortedBucketIO.Read[A], b: SortedBucketIO.Read[B], c: SortedBucketIO.Read[C], d: SortedBucketIO.Read[D], e: SortedBucketIO.Read[E], f: SortedBucketIO.Read[F], targetParallelism: TargetParallelism): SortMergeTransform.ReadBuilder[KEY, KEY, Void, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F])] = self.requireNotClosed {
		val tupleTagA = a.getTupleTag
		val tupleTagB = b.getTupleTag
		val tupleTagC = c.getTupleTag
		val tupleTagD = d.getTupleTag
		val tupleTagE = e.getTupleTag
		val tupleTagF = f.getTupleTag
		val fromResult = { (result: CoGbkResult) =>
			(
				result.getAll(tupleTagA).asScala,
				result.getAll(tupleTagB).asScala,
				result.getAll(tupleTagC).asScala,
				result.getAll(tupleTagD).asScala,
				result.getAll(tupleTagE).asScala,
				result.getAll(tupleTagF).asScala
			)
		}
		if (self.isTest) {
			val result = testCoGroup[KEY](a, b, c, d, e, f)
			val keyed = result.map(kv => kv.getKey -> fromResult(kv.getValue))
			new SortMergeTransform.ReadBuilderTest(self, keyed)
		} else {
			val transform = SortedBucketIO
				.read(keyClass)
				.of(a, b, c, d, e, f)
				.withTargetParallelism(targetParallelism)
			new SortMergeTransform.ReadBuilderImpl(self, transform, fromResult)
		}
	}

	def sortMergeTransform[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder](keyClass: Class[KEY], a: SortedBucketIO.Read[A], b: SortedBucketIO.Read[B], c: SortedBucketIO.Read[C], d: SortedBucketIO.Read[D], e: SortedBucketIO.Read[E], f: SortedBucketIO.Read[F]): SortMergeTransform.ReadBuilder[KEY, KEY, Void, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F])] = {
		sortMergeTransform(keyClass, a, b, c, d, e, f, TargetParallelism.auto())
	}

	def sortMergeTransform[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder](keyClass: Class[KEY], a: SortedBucketIO.Read[A], b: SortedBucketIO.Read[B], c: SortedBucketIO.Read[C], d: SortedBucketIO.Read[D], e: SortedBucketIO.Read[E], f: SortedBucketIO.Read[F], g: SortedBucketIO.Read[G], targetParallelism: TargetParallelism): SortMergeTransform.ReadBuilder[KEY, KEY, Void, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G])] = self.requireNotClosed {
		val tupleTagA = a.getTupleTag
		val tupleTagB = b.getTupleTag
		val tupleTagC = c.getTupleTag
		val tupleTagD = d.getTupleTag
		val tupleTagE = e.getTupleTag
		val tupleTagF = f.getTupleTag
		val tupleTagG = g.getTupleTag
		val fromResult = { (result: CoGbkResult) =>
			(
				result.getAll(tupleTagA).asScala,
				result.getAll(tupleTagB).asScala,
				result.getAll(tupleTagC).asScala,
				result.getAll(tupleTagD).asScala,
				result.getAll(tupleTagE).asScala,
				result.getAll(tupleTagF).asScala,
				result.getAll(tupleTagG).asScala
			)
		}
		if (self.isTest) {
			val result = testCoGroup[KEY](a, b, c, d, e, f, g)
			val keyed = result.map(kv => kv.getKey -> fromResult(kv.getValue))
			new SortMergeTransform.ReadBuilderTest(self, keyed)
		} else {
			val transform = SortedBucketIO
				.read(keyClass)
				.of(a, b, c, d, e, f, g)
				.withTargetParallelism(targetParallelism)
			new SortMergeTransform.ReadBuilderImpl(self, transform, fromResult)
		}
	}

	def sortMergeTransform[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder](keyClass: Class[KEY], a: SortedBucketIO.Read[A], b: SortedBucketIO.Read[B], c: SortedBucketIO.Read[C], d: SortedBucketIO.Read[D], e: SortedBucketIO.Read[E], f: SortedBucketIO.Read[F], g: SortedBucketIO.Read[G]): SortMergeTransform.ReadBuilder[KEY, KEY, Void, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G])] = {
		sortMergeTransform(keyClass, a, b, c, d, e, f, g, TargetParallelism.auto())
	}

	def sortMergeTransform[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder](keyClass: Class[KEY], a: SortedBucketIO.Read[A], b: SortedBucketIO.Read[B], c: SortedBucketIO.Read[C], d: SortedBucketIO.Read[D], e: SortedBucketIO.Read[E], f: SortedBucketIO.Read[F], g: SortedBucketIO.Read[G], h: SortedBucketIO.Read[H], targetParallelism: TargetParallelism): SortMergeTransform.ReadBuilder[KEY, KEY, Void, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H])] = self.requireNotClosed {
		val tupleTagA = a.getTupleTag
		val tupleTagB = b.getTupleTag
		val tupleTagC = c.getTupleTag
		val tupleTagD = d.getTupleTag
		val tupleTagE = e.getTupleTag
		val tupleTagF = f.getTupleTag
		val tupleTagG = g.getTupleTag
		val tupleTagH = h.getTupleTag
		val fromResult = { (result: CoGbkResult) =>
			(
				result.getAll(tupleTagA).asScala,
				result.getAll(tupleTagB).asScala,
				result.getAll(tupleTagC).asScala,
				result.getAll(tupleTagD).asScala,
				result.getAll(tupleTagE).asScala,
				result.getAll(tupleTagF).asScala,
				result.getAll(tupleTagG).asScala,
				result.getAll(tupleTagH).asScala
			)
		}
		if (self.isTest) {
			val result = testCoGroup[KEY](a, b, c, d, e, f, g, h)
			val keyed = result.map(kv => kv.getKey -> fromResult(kv.getValue))
			new SortMergeTransform.ReadBuilderTest(self, keyed)
		} else {
			val transform = SortedBucketIO
				.read(keyClass)
				.of(a, b, c, d, e, f, g, h)
				.withTargetParallelism(targetParallelism)
			new SortMergeTransform.ReadBuilderImpl(self, transform, fromResult)
		}
	}

	def sortMergeTransform[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder](keyClass: Class[KEY], a: SortedBucketIO.Read[A], b: SortedBucketIO.Read[B], c: SortedBucketIO.Read[C], d: SortedBucketIO.Read[D], e: SortedBucketIO.Read[E], f: SortedBucketIO.Read[F], g: SortedBucketIO.Read[G], h: SortedBucketIO.Read[H]): SortMergeTransform.ReadBuilder[KEY, KEY, Void, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H])] = {
		sortMergeTransform(keyClass, a, b, c, d, e, f, g, h, TargetParallelism.auto())
	}

	def sortMergeTransform[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder](keyClass: Class[KEY], a: SortedBucketIO.Read[A], b: SortedBucketIO.Read[B], c: SortedBucketIO.Read[C], d: SortedBucketIO.Read[D], e: SortedBucketIO.Read[E], f: SortedBucketIO.Read[F], g: SortedBucketIO.Read[G], h: SortedBucketIO.Read[H], i: SortedBucketIO.Read[I], targetParallelism: TargetParallelism): SortMergeTransform.ReadBuilder[KEY, KEY, Void, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I])] = self.requireNotClosed {
		val tupleTagA = a.getTupleTag
		val tupleTagB = b.getTupleTag
		val tupleTagC = c.getTupleTag
		val tupleTagD = d.getTupleTag
		val tupleTagE = e.getTupleTag
		val tupleTagF = f.getTupleTag
		val tupleTagG = g.getTupleTag
		val tupleTagH = h.getTupleTag
		val tupleTagI = i.getTupleTag
		val fromResult = { (result: CoGbkResult) =>
			(
				result.getAll(tupleTagA).asScala,
				result.getAll(tupleTagB).asScala,
				result.getAll(tupleTagC).asScala,
				result.getAll(tupleTagD).asScala,
				result.getAll(tupleTagE).asScala,
				result.getAll(tupleTagF).asScala,
				result.getAll(tupleTagG).asScala,
				result.getAll(tupleTagH).asScala,
				result.getAll(tupleTagI).asScala
			)
		}
		if (self.isTest) {
			val result = testCoGroup[KEY](a, b, c, d, e, f, g, h, i)
			val keyed = result.map(kv => kv.getKey -> fromResult(kv.getValue))
			new SortMergeTransform.ReadBuilderTest(self, keyed)
		} else {
			val transform = SortedBucketIO
				.read(keyClass)
				.of(a, b, c, d, e, f, g, h, i)
				.withTargetParallelism(targetParallelism)
			new SortMergeTransform.ReadBuilderImpl(self, transform, fromResult)
		}
	}

	def sortMergeTransform[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder](keyClass: Class[KEY], a: SortedBucketIO.Read[A], b: SortedBucketIO.Read[B], c: SortedBucketIO.Read[C], d: SortedBucketIO.Read[D], e: SortedBucketIO.Read[E], f: SortedBucketIO.Read[F], g: SortedBucketIO.Read[G], h: SortedBucketIO.Read[H], i: SortedBucketIO.Read[I]): SortMergeTransform.ReadBuilder[KEY, KEY, Void, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I])] = {
		sortMergeTransform(keyClass, a, b, c, d, e, f, g, h, i, TargetParallelism.auto())
	}

	def sortMergeTransform[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder](keyClass: Class[KEY], a: SortedBucketIO.Read[A], b: SortedBucketIO.Read[B], c: SortedBucketIO.Read[C], d: SortedBucketIO.Read[D], e: SortedBucketIO.Read[E], f: SortedBucketIO.Read[F], g: SortedBucketIO.Read[G], h: SortedBucketIO.Read[H], i: SortedBucketIO.Read[I], j: SortedBucketIO.Read[J], targetParallelism: TargetParallelism): SortMergeTransform.ReadBuilder[KEY, KEY, Void, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J])] = self.requireNotClosed {
		val tupleTagA = a.getTupleTag
		val tupleTagB = b.getTupleTag
		val tupleTagC = c.getTupleTag
		val tupleTagD = d.getTupleTag
		val tupleTagE = e.getTupleTag
		val tupleTagF = f.getTupleTag
		val tupleTagG = g.getTupleTag
		val tupleTagH = h.getTupleTag
		val tupleTagI = i.getTupleTag
		val tupleTagJ = j.getTupleTag
		val fromResult = { (result: CoGbkResult) =>
			(
				result.getAll(tupleTagA).asScala,
				result.getAll(tupleTagB).asScala,
				result.getAll(tupleTagC).asScala,
				result.getAll(tupleTagD).asScala,
				result.getAll(tupleTagE).asScala,
				result.getAll(tupleTagF).asScala,
				result.getAll(tupleTagG).asScala,
				result.getAll(tupleTagH).asScala,
				result.getAll(tupleTagI).asScala,
				result.getAll(tupleTagJ).asScala
			)
		}
		if (self.isTest) {
			val result = testCoGroup[KEY](a, b, c, d, e, f, g, h, i, j)
			val keyed = result.map(kv => kv.getKey -> fromResult(kv.getValue))
			new SortMergeTransform.ReadBuilderTest(self, keyed)
		} else {
			val transform = SortedBucketIO
				.read(keyClass)
				.of(a, b, c, d, e, f, g, h, i, j)
				.withTargetParallelism(targetParallelism)
			new SortMergeTransform.ReadBuilderImpl(self, transform, fromResult)
		}
	}

	def sortMergeTransform[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder](keyClass: Class[KEY], a: SortedBucketIO.Read[A], b: SortedBucketIO.Read[B], c: SortedBucketIO.Read[C], d: SortedBucketIO.Read[D], e: SortedBucketIO.Read[E], f: SortedBucketIO.Read[F], g: SortedBucketIO.Read[G], h: SortedBucketIO.Read[H], i: SortedBucketIO.Read[I], j: SortedBucketIO.Read[J]): SortMergeTransform.ReadBuilder[KEY, KEY, Void, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J])] = {
		sortMergeTransform(keyClass, a, b, c, d, e, f, g, h, i, j, TargetParallelism.auto())
	}

	def sortMergeTransform[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder](keyClass: Class[KEY], a: SortedBucketIO.Read[A], b: SortedBucketIO.Read[B], c: SortedBucketIO.Read[C], d: SortedBucketIO.Read[D], e: SortedBucketIO.Read[E], f: SortedBucketIO.Read[F], g: SortedBucketIO.Read[G], h: SortedBucketIO.Read[H], i: SortedBucketIO.Read[I], j: SortedBucketIO.Read[J], k: SortedBucketIO.Read[K], targetParallelism: TargetParallelism): SortMergeTransform.ReadBuilder[KEY, KEY, Void, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J], Iterable[K])] = self.requireNotClosed {
		val tupleTagA = a.getTupleTag
		val tupleTagB = b.getTupleTag
		val tupleTagC = c.getTupleTag
		val tupleTagD = d.getTupleTag
		val tupleTagE = e.getTupleTag
		val tupleTagF = f.getTupleTag
		val tupleTagG = g.getTupleTag
		val tupleTagH = h.getTupleTag
		val tupleTagI = i.getTupleTag
		val tupleTagJ = j.getTupleTag
		val tupleTagK = k.getTupleTag
		val fromResult = { (result: CoGbkResult) =>
			(
				result.getAll(tupleTagA).asScala,
				result.getAll(tupleTagB).asScala,
				result.getAll(tupleTagC).asScala,
				result.getAll(tupleTagD).asScala,
				result.getAll(tupleTagE).asScala,
				result.getAll(tupleTagF).asScala,
				result.getAll(tupleTagG).asScala,
				result.getAll(tupleTagH).asScala,
				result.getAll(tupleTagI).asScala,
				result.getAll(tupleTagJ).asScala,
				result.getAll(tupleTagK).asScala
			)
		}
		if (self.isTest) {
			val result = testCoGroup[KEY](a, b, c, d, e, f, g, h, i, j, k)
			val keyed = result.map(kv => kv.getKey -> fromResult(kv.getValue))
			new SortMergeTransform.ReadBuilderTest(self, keyed)
		} else {
			val transform = SortedBucketIO
				.read(keyClass)
				.of(a, b, c, d, e, f, g, h, i, j, k)
				.withTargetParallelism(targetParallelism)
			new SortMergeTransform.ReadBuilderImpl(self, transform, fromResult)
		}
	}

	def sortMergeTransform[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder](keyClass: Class[KEY], a: SortedBucketIO.Read[A], b: SortedBucketIO.Read[B], c: SortedBucketIO.Read[C], d: SortedBucketIO.Read[D], e: SortedBucketIO.Read[E], f: SortedBucketIO.Read[F], g: SortedBucketIO.Read[G], h: SortedBucketIO.Read[H], i: SortedBucketIO.Read[I], j: SortedBucketIO.Read[J], k: SortedBucketIO.Read[K]): SortMergeTransform.ReadBuilder[KEY, KEY, Void, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J], Iterable[K])] = {
		sortMergeTransform(keyClass, a, b, c, d, e, f, g, h, i, j, k, TargetParallelism.auto())
	}

	def sortMergeTransform[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder](keyClass: Class[KEY], a: SortedBucketIO.Read[A], b: SortedBucketIO.Read[B], c: SortedBucketIO.Read[C], d: SortedBucketIO.Read[D], e: SortedBucketIO.Read[E], f: SortedBucketIO.Read[F], g: SortedBucketIO.Read[G], h: SortedBucketIO.Read[H], i: SortedBucketIO.Read[I], j: SortedBucketIO.Read[J], k: SortedBucketIO.Read[K], l: SortedBucketIO.Read[L], targetParallelism: TargetParallelism): SortMergeTransform.ReadBuilder[KEY, KEY, Void, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J], Iterable[K], Iterable[L])] = self.requireNotClosed {
		val tupleTagA = a.getTupleTag
		val tupleTagB = b.getTupleTag
		val tupleTagC = c.getTupleTag
		val tupleTagD = d.getTupleTag
		val tupleTagE = e.getTupleTag
		val tupleTagF = f.getTupleTag
		val tupleTagG = g.getTupleTag
		val tupleTagH = h.getTupleTag
		val tupleTagI = i.getTupleTag
		val tupleTagJ = j.getTupleTag
		val tupleTagK = k.getTupleTag
		val tupleTagL = l.getTupleTag
		val fromResult = { (result: CoGbkResult) =>
			(
				result.getAll(tupleTagA).asScala,
				result.getAll(tupleTagB).asScala,
				result.getAll(tupleTagC).asScala,
				result.getAll(tupleTagD).asScala,
				result.getAll(tupleTagE).asScala,
				result.getAll(tupleTagF).asScala,
				result.getAll(tupleTagG).asScala,
				result.getAll(tupleTagH).asScala,
				result.getAll(tupleTagI).asScala,
				result.getAll(tupleTagJ).asScala,
				result.getAll(tupleTagK).asScala,
				result.getAll(tupleTagL).asScala
			)
		}
		if (self.isTest) {
			val result = testCoGroup[KEY](a, b, c, d, e, f, g, h, i, j, k, l)
			val keyed = result.map(kv => kv.getKey -> fromResult(kv.getValue))
			new SortMergeTransform.ReadBuilderTest(self, keyed)
		} else {
			val transform = SortedBucketIO
				.read(keyClass)
				.of(a, b, c, d, e, f, g, h, i, j, k, l)
				.withTargetParallelism(targetParallelism)
			new SortMergeTransform.ReadBuilderImpl(self, transform, fromResult)
		}
	}

	def sortMergeTransform[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder](keyClass: Class[KEY], a: SortedBucketIO.Read[A], b: SortedBucketIO.Read[B], c: SortedBucketIO.Read[C], d: SortedBucketIO.Read[D], e: SortedBucketIO.Read[E], f: SortedBucketIO.Read[F], g: SortedBucketIO.Read[G], h: SortedBucketIO.Read[H], i: SortedBucketIO.Read[I], j: SortedBucketIO.Read[J], k: SortedBucketIO.Read[K], l: SortedBucketIO.Read[L]): SortMergeTransform.ReadBuilder[KEY, KEY, Void, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J], Iterable[K], Iterable[L])] = {
		sortMergeTransform(keyClass, a, b, c, d, e, f, g, h, i, j, k, l, TargetParallelism.auto())
	}

	def sortMergeTransform[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder](keyClass: Class[KEY], a: SortedBucketIO.Read[A], b: SortedBucketIO.Read[B], c: SortedBucketIO.Read[C], d: SortedBucketIO.Read[D], e: SortedBucketIO.Read[E], f: SortedBucketIO.Read[F], g: SortedBucketIO.Read[G], h: SortedBucketIO.Read[H], i: SortedBucketIO.Read[I], j: SortedBucketIO.Read[J], k: SortedBucketIO.Read[K], l: SortedBucketIO.Read[L], m: SortedBucketIO.Read[M], targetParallelism: TargetParallelism): SortMergeTransform.ReadBuilder[KEY, KEY, Void, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J], Iterable[K], Iterable[L], Iterable[M])] = self.requireNotClosed {
		val tupleTagA = a.getTupleTag
		val tupleTagB = b.getTupleTag
		val tupleTagC = c.getTupleTag
		val tupleTagD = d.getTupleTag
		val tupleTagE = e.getTupleTag
		val tupleTagF = f.getTupleTag
		val tupleTagG = g.getTupleTag
		val tupleTagH = h.getTupleTag
		val tupleTagI = i.getTupleTag
		val tupleTagJ = j.getTupleTag
		val tupleTagK = k.getTupleTag
		val tupleTagL = l.getTupleTag
		val tupleTagM = m.getTupleTag
		val fromResult = { (result: CoGbkResult) =>
			(
				result.getAll(tupleTagA).asScala,
				result.getAll(tupleTagB).asScala,
				result.getAll(tupleTagC).asScala,
				result.getAll(tupleTagD).asScala,
				result.getAll(tupleTagE).asScala,
				result.getAll(tupleTagF).asScala,
				result.getAll(tupleTagG).asScala,
				result.getAll(tupleTagH).asScala,
				result.getAll(tupleTagI).asScala,
				result.getAll(tupleTagJ).asScala,
				result.getAll(tupleTagK).asScala,
				result.getAll(tupleTagL).asScala,
				result.getAll(tupleTagM).asScala
			)
		}
		if (self.isTest) {
			val result = testCoGroup[KEY](a, b, c, d, e, f, g, h, i, j, k, l, m)
			val keyed = result.map(kv => kv.getKey -> fromResult(kv.getValue))
			new SortMergeTransform.ReadBuilderTest(self, keyed)
		} else {
			val transform = SortedBucketIO
				.read(keyClass)
				.of(a, b, c, d, e, f, g, h, i, j, k, l, m)
				.withTargetParallelism(targetParallelism)
			new SortMergeTransform.ReadBuilderImpl(self, transform, fromResult)
		}
	}

	def sortMergeTransform[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder](keyClass: Class[KEY], a: SortedBucketIO.Read[A], b: SortedBucketIO.Read[B], c: SortedBucketIO.Read[C], d: SortedBucketIO.Read[D], e: SortedBucketIO.Read[E], f: SortedBucketIO.Read[F], g: SortedBucketIO.Read[G], h: SortedBucketIO.Read[H], i: SortedBucketIO.Read[I], j: SortedBucketIO.Read[J], k: SortedBucketIO.Read[K], l: SortedBucketIO.Read[L], m: SortedBucketIO.Read[M]): SortMergeTransform.ReadBuilder[KEY, KEY, Void, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J], Iterable[K], Iterable[L], Iterable[M])] = {
		sortMergeTransform(keyClass, a, b, c, d, e, f, g, h, i, j, k, l, m, TargetParallelism.auto())
	}

	def sortMergeTransform[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder, N: Coder](keyClass: Class[KEY], a: SortedBucketIO.Read[A], b: SortedBucketIO.Read[B], c: SortedBucketIO.Read[C], d: SortedBucketIO.Read[D], e: SortedBucketIO.Read[E], f: SortedBucketIO.Read[F], g: SortedBucketIO.Read[G], h: SortedBucketIO.Read[H], i: SortedBucketIO.Read[I], j: SortedBucketIO.Read[J], k: SortedBucketIO.Read[K], l: SortedBucketIO.Read[L], m: SortedBucketIO.Read[M], n: SortedBucketIO.Read[N], targetParallelism: TargetParallelism): SortMergeTransform.ReadBuilder[KEY, KEY, Void, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J], Iterable[K], Iterable[L], Iterable[M], Iterable[N])] = self.requireNotClosed {
		val tupleTagA = a.getTupleTag
		val tupleTagB = b.getTupleTag
		val tupleTagC = c.getTupleTag
		val tupleTagD = d.getTupleTag
		val tupleTagE = e.getTupleTag
		val tupleTagF = f.getTupleTag
		val tupleTagG = g.getTupleTag
		val tupleTagH = h.getTupleTag
		val tupleTagI = i.getTupleTag
		val tupleTagJ = j.getTupleTag
		val tupleTagK = k.getTupleTag
		val tupleTagL = l.getTupleTag
		val tupleTagM = m.getTupleTag
		val tupleTagN = n.getTupleTag
		val fromResult = { (result: CoGbkResult) =>
			(
				result.getAll(tupleTagA).asScala,
				result.getAll(tupleTagB).asScala,
				result.getAll(tupleTagC).asScala,
				result.getAll(tupleTagD).asScala,
				result.getAll(tupleTagE).asScala,
				result.getAll(tupleTagF).asScala,
				result.getAll(tupleTagG).asScala,
				result.getAll(tupleTagH).asScala,
				result.getAll(tupleTagI).asScala,
				result.getAll(tupleTagJ).asScala,
				result.getAll(tupleTagK).asScala,
				result.getAll(tupleTagL).asScala,
				result.getAll(tupleTagM).asScala,
				result.getAll(tupleTagN).asScala
			)
		}
		if (self.isTest) {
			val result = testCoGroup[KEY](a, b, c, d, e, f, g, h, i, j, k, l, m, n)
			val keyed = result.map(kv => kv.getKey -> fromResult(kv.getValue))
			new SortMergeTransform.ReadBuilderTest(self, keyed)
		} else {
			val transform = SortedBucketIO
				.read(keyClass)
				.of(a, b, c, d, e, f, g, h, i, j, k, l, m, n)
				.withTargetParallelism(targetParallelism)
			new SortMergeTransform.ReadBuilderImpl(self, transform, fromResult)
		}
	}

	def sortMergeTransform[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder, N: Coder](keyClass: Class[KEY], a: SortedBucketIO.Read[A], b: SortedBucketIO.Read[B], c: SortedBucketIO.Read[C], d: SortedBucketIO.Read[D], e: SortedBucketIO.Read[E], f: SortedBucketIO.Read[F], g: SortedBucketIO.Read[G], h: SortedBucketIO.Read[H], i: SortedBucketIO.Read[I], j: SortedBucketIO.Read[J], k: SortedBucketIO.Read[K], l: SortedBucketIO.Read[L], m: SortedBucketIO.Read[M], n: SortedBucketIO.Read[N]): SortMergeTransform.ReadBuilder[KEY, KEY, Void, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J], Iterable[K], Iterable[L], Iterable[M], Iterable[N])] = {
		sortMergeTransform(keyClass, a, b, c, d, e, f, g, h, i, j, k, l, m, n, TargetParallelism.auto())
	}

	def sortMergeTransform[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder, N: Coder, O: Coder](keyClass: Class[KEY], a: SortedBucketIO.Read[A], b: SortedBucketIO.Read[B], c: SortedBucketIO.Read[C], d: SortedBucketIO.Read[D], e: SortedBucketIO.Read[E], f: SortedBucketIO.Read[F], g: SortedBucketIO.Read[G], h: SortedBucketIO.Read[H], i: SortedBucketIO.Read[I], j: SortedBucketIO.Read[J], k: SortedBucketIO.Read[K], l: SortedBucketIO.Read[L], m: SortedBucketIO.Read[M], n: SortedBucketIO.Read[N], o: SortedBucketIO.Read[O], targetParallelism: TargetParallelism): SortMergeTransform.ReadBuilder[KEY, KEY, Void, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J], Iterable[K], Iterable[L], Iterable[M], Iterable[N], Iterable[O])] = self.requireNotClosed {
		val tupleTagA = a.getTupleTag
		val tupleTagB = b.getTupleTag
		val tupleTagC = c.getTupleTag
		val tupleTagD = d.getTupleTag
		val tupleTagE = e.getTupleTag
		val tupleTagF = f.getTupleTag
		val tupleTagG = g.getTupleTag
		val tupleTagH = h.getTupleTag
		val tupleTagI = i.getTupleTag
		val tupleTagJ = j.getTupleTag
		val tupleTagK = k.getTupleTag
		val tupleTagL = l.getTupleTag
		val tupleTagM = m.getTupleTag
		val tupleTagN = n.getTupleTag
		val tupleTagO = o.getTupleTag
		val fromResult = { (result: CoGbkResult) =>
			(
				result.getAll(tupleTagA).asScala,
				result.getAll(tupleTagB).asScala,
				result.getAll(tupleTagC).asScala,
				result.getAll(tupleTagD).asScala,
				result.getAll(tupleTagE).asScala,
				result.getAll(tupleTagF).asScala,
				result.getAll(tupleTagG).asScala,
				result.getAll(tupleTagH).asScala,
				result.getAll(tupleTagI).asScala,
				result.getAll(tupleTagJ).asScala,
				result.getAll(tupleTagK).asScala,
				result.getAll(tupleTagL).asScala,
				result.getAll(tupleTagM).asScala,
				result.getAll(tupleTagN).asScala,
				result.getAll(tupleTagO).asScala
			)
		}
		if (self.isTest) {
			val result = testCoGroup[KEY](a, b, c, d, e, f, g, h, i, j, k, l, m, n, o)
			val keyed = result.map(kv => kv.getKey -> fromResult(kv.getValue))
			new SortMergeTransform.ReadBuilderTest(self, keyed)
		} else {
			val transform = SortedBucketIO
				.read(keyClass)
				.of(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o)
				.withTargetParallelism(targetParallelism)
			new SortMergeTransform.ReadBuilderImpl(self, transform, fromResult)
		}
	}

	def sortMergeTransform[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder, N: Coder, O: Coder](keyClass: Class[KEY], a: SortedBucketIO.Read[A], b: SortedBucketIO.Read[B], c: SortedBucketIO.Read[C], d: SortedBucketIO.Read[D], e: SortedBucketIO.Read[E], f: SortedBucketIO.Read[F], g: SortedBucketIO.Read[G], h: SortedBucketIO.Read[H], i: SortedBucketIO.Read[I], j: SortedBucketIO.Read[J], k: SortedBucketIO.Read[K], l: SortedBucketIO.Read[L], m: SortedBucketIO.Read[M], n: SortedBucketIO.Read[N], o: SortedBucketIO.Read[O]): SortMergeTransform.ReadBuilder[KEY, KEY, Void, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J], Iterable[K], Iterable[L], Iterable[M], Iterable[N], Iterable[O])] = {
		sortMergeTransform(keyClass, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, TargetParallelism.auto())
	}

	def sortMergeTransform[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder, N: Coder, O: Coder, P: Coder](keyClass: Class[KEY], a: SortedBucketIO.Read[A], b: SortedBucketIO.Read[B], c: SortedBucketIO.Read[C], d: SortedBucketIO.Read[D], e: SortedBucketIO.Read[E], f: SortedBucketIO.Read[F], g: SortedBucketIO.Read[G], h: SortedBucketIO.Read[H], i: SortedBucketIO.Read[I], j: SortedBucketIO.Read[J], k: SortedBucketIO.Read[K], l: SortedBucketIO.Read[L], m: SortedBucketIO.Read[M], n: SortedBucketIO.Read[N], o: SortedBucketIO.Read[O], p: SortedBucketIO.Read[P], targetParallelism: TargetParallelism): SortMergeTransform.ReadBuilder[KEY, KEY, Void, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J], Iterable[K], Iterable[L], Iterable[M], Iterable[N], Iterable[O], Iterable[P])] = self.requireNotClosed {
		val tupleTagA = a.getTupleTag
		val tupleTagB = b.getTupleTag
		val tupleTagC = c.getTupleTag
		val tupleTagD = d.getTupleTag
		val tupleTagE = e.getTupleTag
		val tupleTagF = f.getTupleTag
		val tupleTagG = g.getTupleTag
		val tupleTagH = h.getTupleTag
		val tupleTagI = i.getTupleTag
		val tupleTagJ = j.getTupleTag
		val tupleTagK = k.getTupleTag
		val tupleTagL = l.getTupleTag
		val tupleTagM = m.getTupleTag
		val tupleTagN = n.getTupleTag
		val tupleTagO = o.getTupleTag
		val tupleTagP = p.getTupleTag
		val fromResult = { (result: CoGbkResult) =>
			(
				result.getAll(tupleTagA).asScala,
				result.getAll(tupleTagB).asScala,
				result.getAll(tupleTagC).asScala,
				result.getAll(tupleTagD).asScala,
				result.getAll(tupleTagE).asScala,
				result.getAll(tupleTagF).asScala,
				result.getAll(tupleTagG).asScala,
				result.getAll(tupleTagH).asScala,
				result.getAll(tupleTagI).asScala,
				result.getAll(tupleTagJ).asScala,
				result.getAll(tupleTagK).asScala,
				result.getAll(tupleTagL).asScala,
				result.getAll(tupleTagM).asScala,
				result.getAll(tupleTagN).asScala,
				result.getAll(tupleTagO).asScala,
				result.getAll(tupleTagP).asScala
			)
		}
		if (self.isTest) {
			val result = testCoGroup[KEY](a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p)
			val keyed = result.map(kv => kv.getKey -> fromResult(kv.getValue))
			new SortMergeTransform.ReadBuilderTest(self, keyed)
		} else {
			val transform = SortedBucketIO
				.read(keyClass)
				.of(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p)
				.withTargetParallelism(targetParallelism)
			new SortMergeTransform.ReadBuilderImpl(self, transform, fromResult)
		}
	}

	def sortMergeTransform[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder, N: Coder, O: Coder, P: Coder](keyClass: Class[KEY], a: SortedBucketIO.Read[A], b: SortedBucketIO.Read[B], c: SortedBucketIO.Read[C], d: SortedBucketIO.Read[D], e: SortedBucketIO.Read[E], f: SortedBucketIO.Read[F], g: SortedBucketIO.Read[G], h: SortedBucketIO.Read[H], i: SortedBucketIO.Read[I], j: SortedBucketIO.Read[J], k: SortedBucketIO.Read[K], l: SortedBucketIO.Read[L], m: SortedBucketIO.Read[M], n: SortedBucketIO.Read[N], o: SortedBucketIO.Read[O], p: SortedBucketIO.Read[P]): SortMergeTransform.ReadBuilder[KEY, KEY, Void, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J], Iterable[K], Iterable[L], Iterable[M], Iterable[N], Iterable[O], Iterable[P])] = {
		sortMergeTransform(keyClass, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, TargetParallelism.auto())
	}

	def sortMergeTransform[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder, N: Coder, O: Coder, P: Coder, Q: Coder](keyClass: Class[KEY], a: SortedBucketIO.Read[A], b: SortedBucketIO.Read[B], c: SortedBucketIO.Read[C], d: SortedBucketIO.Read[D], e: SortedBucketIO.Read[E], f: SortedBucketIO.Read[F], g: SortedBucketIO.Read[G], h: SortedBucketIO.Read[H], i: SortedBucketIO.Read[I], j: SortedBucketIO.Read[J], k: SortedBucketIO.Read[K], l: SortedBucketIO.Read[L], m: SortedBucketIO.Read[M], n: SortedBucketIO.Read[N], o: SortedBucketIO.Read[O], p: SortedBucketIO.Read[P], q: SortedBucketIO.Read[Q], targetParallelism: TargetParallelism): SortMergeTransform.ReadBuilder[KEY, KEY, Void, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J], Iterable[K], Iterable[L], Iterable[M], Iterable[N], Iterable[O], Iterable[P], Iterable[Q])] = self.requireNotClosed {
		val tupleTagA = a.getTupleTag
		val tupleTagB = b.getTupleTag
		val tupleTagC = c.getTupleTag
		val tupleTagD = d.getTupleTag
		val tupleTagE = e.getTupleTag
		val tupleTagF = f.getTupleTag
		val tupleTagG = g.getTupleTag
		val tupleTagH = h.getTupleTag
		val tupleTagI = i.getTupleTag
		val tupleTagJ = j.getTupleTag
		val tupleTagK = k.getTupleTag
		val tupleTagL = l.getTupleTag
		val tupleTagM = m.getTupleTag
		val tupleTagN = n.getTupleTag
		val tupleTagO = o.getTupleTag
		val tupleTagP = p.getTupleTag
		val tupleTagQ = q.getTupleTag
		val fromResult = { (result: CoGbkResult) =>
			(
				result.getAll(tupleTagA).asScala,
				result.getAll(tupleTagB).asScala,
				result.getAll(tupleTagC).asScala,
				result.getAll(tupleTagD).asScala,
				result.getAll(tupleTagE).asScala,
				result.getAll(tupleTagF).asScala,
				result.getAll(tupleTagG).asScala,
				result.getAll(tupleTagH).asScala,
				result.getAll(tupleTagI).asScala,
				result.getAll(tupleTagJ).asScala,
				result.getAll(tupleTagK).asScala,
				result.getAll(tupleTagL).asScala,
				result.getAll(tupleTagM).asScala,
				result.getAll(tupleTagN).asScala,
				result.getAll(tupleTagO).asScala,
				result.getAll(tupleTagP).asScala,
				result.getAll(tupleTagQ).asScala
			)
		}
		if (self.isTest) {
			val result = testCoGroup[KEY](a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q)
			val keyed = result.map(kv => kv.getKey -> fromResult(kv.getValue))
			new SortMergeTransform.ReadBuilderTest(self, keyed)
		} else {
			val transform = SortedBucketIO
				.read(keyClass)
				.of(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q)
				.withTargetParallelism(targetParallelism)
			new SortMergeTransform.ReadBuilderImpl(self, transform, fromResult)
		}
	}

	def sortMergeTransform[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder, N: Coder, O: Coder, P: Coder, Q: Coder](keyClass: Class[KEY], a: SortedBucketIO.Read[A], b: SortedBucketIO.Read[B], c: SortedBucketIO.Read[C], d: SortedBucketIO.Read[D], e: SortedBucketIO.Read[E], f: SortedBucketIO.Read[F], g: SortedBucketIO.Read[G], h: SortedBucketIO.Read[H], i: SortedBucketIO.Read[I], j: SortedBucketIO.Read[J], k: SortedBucketIO.Read[K], l: SortedBucketIO.Read[L], m: SortedBucketIO.Read[M], n: SortedBucketIO.Read[N], o: SortedBucketIO.Read[O], p: SortedBucketIO.Read[P], q: SortedBucketIO.Read[Q]): SortMergeTransform.ReadBuilder[KEY, KEY, Void, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J], Iterable[K], Iterable[L], Iterable[M], Iterable[N], Iterable[O], Iterable[P], Iterable[Q])] = {
		sortMergeTransform(keyClass, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, TargetParallelism.auto())
	}

	def sortMergeTransform[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder, N: Coder, O: Coder, P: Coder, Q: Coder, R: Coder](keyClass: Class[KEY], a: SortedBucketIO.Read[A], b: SortedBucketIO.Read[B], c: SortedBucketIO.Read[C], d: SortedBucketIO.Read[D], e: SortedBucketIO.Read[E], f: SortedBucketIO.Read[F], g: SortedBucketIO.Read[G], h: SortedBucketIO.Read[H], i: SortedBucketIO.Read[I], j: SortedBucketIO.Read[J], k: SortedBucketIO.Read[K], l: SortedBucketIO.Read[L], m: SortedBucketIO.Read[M], n: SortedBucketIO.Read[N], o: SortedBucketIO.Read[O], p: SortedBucketIO.Read[P], q: SortedBucketIO.Read[Q], r: SortedBucketIO.Read[R], targetParallelism: TargetParallelism): SortMergeTransform.ReadBuilder[KEY, KEY, Void, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J], Iterable[K], Iterable[L], Iterable[M], Iterable[N], Iterable[O], Iterable[P], Iterable[Q], Iterable[R])] = self.requireNotClosed {
		val tupleTagA = a.getTupleTag
		val tupleTagB = b.getTupleTag
		val tupleTagC = c.getTupleTag
		val tupleTagD = d.getTupleTag
		val tupleTagE = e.getTupleTag
		val tupleTagF = f.getTupleTag
		val tupleTagG = g.getTupleTag
		val tupleTagH = h.getTupleTag
		val tupleTagI = i.getTupleTag
		val tupleTagJ = j.getTupleTag
		val tupleTagK = k.getTupleTag
		val tupleTagL = l.getTupleTag
		val tupleTagM = m.getTupleTag
		val tupleTagN = n.getTupleTag
		val tupleTagO = o.getTupleTag
		val tupleTagP = p.getTupleTag
		val tupleTagQ = q.getTupleTag
		val tupleTagR = r.getTupleTag
		val fromResult = { (result: CoGbkResult) =>
			(
				result.getAll(tupleTagA).asScala,
				result.getAll(tupleTagB).asScala,
				result.getAll(tupleTagC).asScala,
				result.getAll(tupleTagD).asScala,
				result.getAll(tupleTagE).asScala,
				result.getAll(tupleTagF).asScala,
				result.getAll(tupleTagG).asScala,
				result.getAll(tupleTagH).asScala,
				result.getAll(tupleTagI).asScala,
				result.getAll(tupleTagJ).asScala,
				result.getAll(tupleTagK).asScala,
				result.getAll(tupleTagL).asScala,
				result.getAll(tupleTagM).asScala,
				result.getAll(tupleTagN).asScala,
				result.getAll(tupleTagO).asScala,
				result.getAll(tupleTagP).asScala,
				result.getAll(tupleTagQ).asScala,
				result.getAll(tupleTagR).asScala
			)
		}
		if (self.isTest) {
			val result = testCoGroup[KEY](a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r)
			val keyed = result.map(kv => kv.getKey -> fromResult(kv.getValue))
			new SortMergeTransform.ReadBuilderTest(self, keyed)
		} else {
			val transform = SortedBucketIO
				.read(keyClass)
				.of(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r)
				.withTargetParallelism(targetParallelism)
			new SortMergeTransform.ReadBuilderImpl(self, transform, fromResult)
		}
	}

	def sortMergeTransform[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder, N: Coder, O: Coder, P: Coder, Q: Coder, R: Coder](keyClass: Class[KEY], a: SortedBucketIO.Read[A], b: SortedBucketIO.Read[B], c: SortedBucketIO.Read[C], d: SortedBucketIO.Read[D], e: SortedBucketIO.Read[E], f: SortedBucketIO.Read[F], g: SortedBucketIO.Read[G], h: SortedBucketIO.Read[H], i: SortedBucketIO.Read[I], j: SortedBucketIO.Read[J], k: SortedBucketIO.Read[K], l: SortedBucketIO.Read[L], m: SortedBucketIO.Read[M], n: SortedBucketIO.Read[N], o: SortedBucketIO.Read[O], p: SortedBucketIO.Read[P], q: SortedBucketIO.Read[Q], r: SortedBucketIO.Read[R]): SortMergeTransform.ReadBuilder[KEY, KEY, Void, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J], Iterable[K], Iterable[L], Iterable[M], Iterable[N], Iterable[O], Iterable[P], Iterable[Q], Iterable[R])] = {
		sortMergeTransform(keyClass, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, TargetParallelism.auto())
	}

	def sortMergeTransform[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder, N: Coder, O: Coder, P: Coder, Q: Coder, R: Coder, S: Coder](keyClass: Class[KEY], a: SortedBucketIO.Read[A], b: SortedBucketIO.Read[B], c: SortedBucketIO.Read[C], d: SortedBucketIO.Read[D], e: SortedBucketIO.Read[E], f: SortedBucketIO.Read[F], g: SortedBucketIO.Read[G], h: SortedBucketIO.Read[H], i: SortedBucketIO.Read[I], j: SortedBucketIO.Read[J], k: SortedBucketIO.Read[K], l: SortedBucketIO.Read[L], m: SortedBucketIO.Read[M], n: SortedBucketIO.Read[N], o: SortedBucketIO.Read[O], p: SortedBucketIO.Read[P], q: SortedBucketIO.Read[Q], r: SortedBucketIO.Read[R], s: SortedBucketIO.Read[S], targetParallelism: TargetParallelism): SortMergeTransform.ReadBuilder[KEY, KEY, Void, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J], Iterable[K], Iterable[L], Iterable[M], Iterable[N], Iterable[O], Iterable[P], Iterable[Q], Iterable[R], Iterable[S])] = self.requireNotClosed {
		val tupleTagA = a.getTupleTag
		val tupleTagB = b.getTupleTag
		val tupleTagC = c.getTupleTag
		val tupleTagD = d.getTupleTag
		val tupleTagE = e.getTupleTag
		val tupleTagF = f.getTupleTag
		val tupleTagG = g.getTupleTag
		val tupleTagH = h.getTupleTag
		val tupleTagI = i.getTupleTag
		val tupleTagJ = j.getTupleTag
		val tupleTagK = k.getTupleTag
		val tupleTagL = l.getTupleTag
		val tupleTagM = m.getTupleTag
		val tupleTagN = n.getTupleTag
		val tupleTagO = o.getTupleTag
		val tupleTagP = p.getTupleTag
		val tupleTagQ = q.getTupleTag
		val tupleTagR = r.getTupleTag
		val tupleTagS = s.getTupleTag
		val fromResult = { (result: CoGbkResult) =>
			(
				result.getAll(tupleTagA).asScala,
				result.getAll(tupleTagB).asScala,
				result.getAll(tupleTagC).asScala,
				result.getAll(tupleTagD).asScala,
				result.getAll(tupleTagE).asScala,
				result.getAll(tupleTagF).asScala,
				result.getAll(tupleTagG).asScala,
				result.getAll(tupleTagH).asScala,
				result.getAll(tupleTagI).asScala,
				result.getAll(tupleTagJ).asScala,
				result.getAll(tupleTagK).asScala,
				result.getAll(tupleTagL).asScala,
				result.getAll(tupleTagM).asScala,
				result.getAll(tupleTagN).asScala,
				result.getAll(tupleTagO).asScala,
				result.getAll(tupleTagP).asScala,
				result.getAll(tupleTagQ).asScala,
				result.getAll(tupleTagR).asScala,
				result.getAll(tupleTagS).asScala
			)
		}
		if (self.isTest) {
			val result = testCoGroup[KEY](a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s)
			val keyed = result.map(kv => kv.getKey -> fromResult(kv.getValue))
			new SortMergeTransform.ReadBuilderTest(self, keyed)
		} else {
			val transform = SortedBucketIO
				.read(keyClass)
				.of(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s)
				.withTargetParallelism(targetParallelism)
			new SortMergeTransform.ReadBuilderImpl(self, transform, fromResult)
		}
	}

	def sortMergeTransform[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder, N: Coder, O: Coder, P: Coder, Q: Coder, R: Coder, S: Coder](keyClass: Class[KEY], a: SortedBucketIO.Read[A], b: SortedBucketIO.Read[B], c: SortedBucketIO.Read[C], d: SortedBucketIO.Read[D], e: SortedBucketIO.Read[E], f: SortedBucketIO.Read[F], g: SortedBucketIO.Read[G], h: SortedBucketIO.Read[H], i: SortedBucketIO.Read[I], j: SortedBucketIO.Read[J], k: SortedBucketIO.Read[K], l: SortedBucketIO.Read[L], m: SortedBucketIO.Read[M], n: SortedBucketIO.Read[N], o: SortedBucketIO.Read[O], p: SortedBucketIO.Read[P], q: SortedBucketIO.Read[Q], r: SortedBucketIO.Read[R], s: SortedBucketIO.Read[S]): SortMergeTransform.ReadBuilder[KEY, KEY, Void, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J], Iterable[K], Iterable[L], Iterable[M], Iterable[N], Iterable[O], Iterable[P], Iterable[Q], Iterable[R], Iterable[S])] = {
		sortMergeTransform(keyClass, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, TargetParallelism.auto())
	}

	def sortMergeTransform[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder, N: Coder, O: Coder, P: Coder, Q: Coder, R: Coder, S: Coder, T: Coder](keyClass: Class[KEY], a: SortedBucketIO.Read[A], b: SortedBucketIO.Read[B], c: SortedBucketIO.Read[C], d: SortedBucketIO.Read[D], e: SortedBucketIO.Read[E], f: SortedBucketIO.Read[F], g: SortedBucketIO.Read[G], h: SortedBucketIO.Read[H], i: SortedBucketIO.Read[I], j: SortedBucketIO.Read[J], k: SortedBucketIO.Read[K], l: SortedBucketIO.Read[L], m: SortedBucketIO.Read[M], n: SortedBucketIO.Read[N], o: SortedBucketIO.Read[O], p: SortedBucketIO.Read[P], q: SortedBucketIO.Read[Q], r: SortedBucketIO.Read[R], s: SortedBucketIO.Read[S], t: SortedBucketIO.Read[T], targetParallelism: TargetParallelism): SortMergeTransform.ReadBuilder[KEY, KEY, Void, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J], Iterable[K], Iterable[L], Iterable[M], Iterable[N], Iterable[O], Iterable[P], Iterable[Q], Iterable[R], Iterable[S], Iterable[T])] = self.requireNotClosed {
		val tupleTagA = a.getTupleTag
		val tupleTagB = b.getTupleTag
		val tupleTagC = c.getTupleTag
		val tupleTagD = d.getTupleTag
		val tupleTagE = e.getTupleTag
		val tupleTagF = f.getTupleTag
		val tupleTagG = g.getTupleTag
		val tupleTagH = h.getTupleTag
		val tupleTagI = i.getTupleTag
		val tupleTagJ = j.getTupleTag
		val tupleTagK = k.getTupleTag
		val tupleTagL = l.getTupleTag
		val tupleTagM = m.getTupleTag
		val tupleTagN = n.getTupleTag
		val tupleTagO = o.getTupleTag
		val tupleTagP = p.getTupleTag
		val tupleTagQ = q.getTupleTag
		val tupleTagR = r.getTupleTag
		val tupleTagS = s.getTupleTag
		val tupleTagT = t.getTupleTag
		val fromResult = { (result: CoGbkResult) =>
			(
				result.getAll(tupleTagA).asScala,
				result.getAll(tupleTagB).asScala,
				result.getAll(tupleTagC).asScala,
				result.getAll(tupleTagD).asScala,
				result.getAll(tupleTagE).asScala,
				result.getAll(tupleTagF).asScala,
				result.getAll(tupleTagG).asScala,
				result.getAll(tupleTagH).asScala,
				result.getAll(tupleTagI).asScala,
				result.getAll(tupleTagJ).asScala,
				result.getAll(tupleTagK).asScala,
				result.getAll(tupleTagL).asScala,
				result.getAll(tupleTagM).asScala,
				result.getAll(tupleTagN).asScala,
				result.getAll(tupleTagO).asScala,
				result.getAll(tupleTagP).asScala,
				result.getAll(tupleTagQ).asScala,
				result.getAll(tupleTagR).asScala,
				result.getAll(tupleTagS).asScala,
				result.getAll(tupleTagT).asScala
			)
		}
		if (self.isTest) {
			val result = testCoGroup[KEY](a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t)
			val keyed = result.map(kv => kv.getKey -> fromResult(kv.getValue))
			new SortMergeTransform.ReadBuilderTest(self, keyed)
		} else {
			val transform = SortedBucketIO
				.read(keyClass)
				.of(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t)
				.withTargetParallelism(targetParallelism)
			new SortMergeTransform.ReadBuilderImpl(self, transform, fromResult)
		}
	}

	def sortMergeTransform[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder, N: Coder, O: Coder, P: Coder, Q: Coder, R: Coder, S: Coder, T: Coder](keyClass: Class[KEY], a: SortedBucketIO.Read[A], b: SortedBucketIO.Read[B], c: SortedBucketIO.Read[C], d: SortedBucketIO.Read[D], e: SortedBucketIO.Read[E], f: SortedBucketIO.Read[F], g: SortedBucketIO.Read[G], h: SortedBucketIO.Read[H], i: SortedBucketIO.Read[I], j: SortedBucketIO.Read[J], k: SortedBucketIO.Read[K], l: SortedBucketIO.Read[L], m: SortedBucketIO.Read[M], n: SortedBucketIO.Read[N], o: SortedBucketIO.Read[O], p: SortedBucketIO.Read[P], q: SortedBucketIO.Read[Q], r: SortedBucketIO.Read[R], s: SortedBucketIO.Read[S], t: SortedBucketIO.Read[T]): SortMergeTransform.ReadBuilder[KEY, KEY, Void, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J], Iterable[K], Iterable[L], Iterable[M], Iterable[N], Iterable[O], Iterable[P], Iterable[Q], Iterable[R], Iterable[S], Iterable[T])] = {
		sortMergeTransform(keyClass, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, TargetParallelism.auto())
	}

	def sortMergeTransform[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder, N: Coder, O: Coder, P: Coder, Q: Coder, R: Coder, S: Coder, T: Coder, U: Coder](keyClass: Class[KEY], a: SortedBucketIO.Read[A], b: SortedBucketIO.Read[B], c: SortedBucketIO.Read[C], d: SortedBucketIO.Read[D], e: SortedBucketIO.Read[E], f: SortedBucketIO.Read[F], g: SortedBucketIO.Read[G], h: SortedBucketIO.Read[H], i: SortedBucketIO.Read[I], j: SortedBucketIO.Read[J], k: SortedBucketIO.Read[K], l: SortedBucketIO.Read[L], m: SortedBucketIO.Read[M], n: SortedBucketIO.Read[N], o: SortedBucketIO.Read[O], p: SortedBucketIO.Read[P], q: SortedBucketIO.Read[Q], r: SortedBucketIO.Read[R], s: SortedBucketIO.Read[S], t: SortedBucketIO.Read[T], u: SortedBucketIO.Read[U], targetParallelism: TargetParallelism): SortMergeTransform.ReadBuilder[KEY, KEY, Void, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J], Iterable[K], Iterable[L], Iterable[M], Iterable[N], Iterable[O], Iterable[P], Iterable[Q], Iterable[R], Iterable[S], Iterable[T], Iterable[U])] = self.requireNotClosed {
		val tupleTagA = a.getTupleTag
		val tupleTagB = b.getTupleTag
		val tupleTagC = c.getTupleTag
		val tupleTagD = d.getTupleTag
		val tupleTagE = e.getTupleTag
		val tupleTagF = f.getTupleTag
		val tupleTagG = g.getTupleTag
		val tupleTagH = h.getTupleTag
		val tupleTagI = i.getTupleTag
		val tupleTagJ = j.getTupleTag
		val tupleTagK = k.getTupleTag
		val tupleTagL = l.getTupleTag
		val tupleTagM = m.getTupleTag
		val tupleTagN = n.getTupleTag
		val tupleTagO = o.getTupleTag
		val tupleTagP = p.getTupleTag
		val tupleTagQ = q.getTupleTag
		val tupleTagR = r.getTupleTag
		val tupleTagS = s.getTupleTag
		val tupleTagT = t.getTupleTag
		val tupleTagU = u.getTupleTag
		val fromResult = { (result: CoGbkResult) =>
			(
				result.getAll(tupleTagA).asScala,
				result.getAll(tupleTagB).asScala,
				result.getAll(tupleTagC).asScala,
				result.getAll(tupleTagD).asScala,
				result.getAll(tupleTagE).asScala,
				result.getAll(tupleTagF).asScala,
				result.getAll(tupleTagG).asScala,
				result.getAll(tupleTagH).asScala,
				result.getAll(tupleTagI).asScala,
				result.getAll(tupleTagJ).asScala,
				result.getAll(tupleTagK).asScala,
				result.getAll(tupleTagL).asScala,
				result.getAll(tupleTagM).asScala,
				result.getAll(tupleTagN).asScala,
				result.getAll(tupleTagO).asScala,
				result.getAll(tupleTagP).asScala,
				result.getAll(tupleTagQ).asScala,
				result.getAll(tupleTagR).asScala,
				result.getAll(tupleTagS).asScala,
				result.getAll(tupleTagT).asScala,
				result.getAll(tupleTagU).asScala
			)
		}
		if (self.isTest) {
			val result = testCoGroup[KEY](a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u)
			val keyed = result.map(kv => kv.getKey -> fromResult(kv.getValue))
			new SortMergeTransform.ReadBuilderTest(self, keyed)
		} else {
			val transform = SortedBucketIO
				.read(keyClass)
				.of(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u)
				.withTargetParallelism(targetParallelism)
			new SortMergeTransform.ReadBuilderImpl(self, transform, fromResult)
		}
	}

	def sortMergeTransform[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder, N: Coder, O: Coder, P: Coder, Q: Coder, R: Coder, S: Coder, T: Coder, U: Coder](keyClass: Class[KEY], a: SortedBucketIO.Read[A], b: SortedBucketIO.Read[B], c: SortedBucketIO.Read[C], d: SortedBucketIO.Read[D], e: SortedBucketIO.Read[E], f: SortedBucketIO.Read[F], g: SortedBucketIO.Read[G], h: SortedBucketIO.Read[H], i: SortedBucketIO.Read[I], j: SortedBucketIO.Read[J], k: SortedBucketIO.Read[K], l: SortedBucketIO.Read[L], m: SortedBucketIO.Read[M], n: SortedBucketIO.Read[N], o: SortedBucketIO.Read[O], p: SortedBucketIO.Read[P], q: SortedBucketIO.Read[Q], r: SortedBucketIO.Read[R], s: SortedBucketIO.Read[S], t: SortedBucketIO.Read[T], u: SortedBucketIO.Read[U]): SortMergeTransform.ReadBuilder[KEY, KEY, Void, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J], Iterable[K], Iterable[L], Iterable[M], Iterable[N], Iterable[O], Iterable[P], Iterable[Q], Iterable[R], Iterable[S], Iterable[T], Iterable[U])] = {
		sortMergeTransform(keyClass, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, TargetParallelism.auto())
	}

	def sortMergeTransform[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder, N: Coder, O: Coder, P: Coder, Q: Coder, R: Coder, S: Coder, T: Coder, U: Coder, V: Coder](keyClass: Class[KEY], a: SortedBucketIO.Read[A], b: SortedBucketIO.Read[B], c: SortedBucketIO.Read[C], d: SortedBucketIO.Read[D], e: SortedBucketIO.Read[E], f: SortedBucketIO.Read[F], g: SortedBucketIO.Read[G], h: SortedBucketIO.Read[H], i: SortedBucketIO.Read[I], j: SortedBucketIO.Read[J], k: SortedBucketIO.Read[K], l: SortedBucketIO.Read[L], m: SortedBucketIO.Read[M], n: SortedBucketIO.Read[N], o: SortedBucketIO.Read[O], p: SortedBucketIO.Read[P], q: SortedBucketIO.Read[Q], r: SortedBucketIO.Read[R], s: SortedBucketIO.Read[S], t: SortedBucketIO.Read[T], u: SortedBucketIO.Read[U], v: SortedBucketIO.Read[V], targetParallelism: TargetParallelism): SortMergeTransform.ReadBuilder[KEY, KEY, Void, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J], Iterable[K], Iterable[L], Iterable[M], Iterable[N], Iterable[O], Iterable[P], Iterable[Q], Iterable[R], Iterable[S], Iterable[T], Iterable[U], Iterable[V])] = self.requireNotClosed {
		val tupleTagA = a.getTupleTag
		val tupleTagB = b.getTupleTag
		val tupleTagC = c.getTupleTag
		val tupleTagD = d.getTupleTag
		val tupleTagE = e.getTupleTag
		val tupleTagF = f.getTupleTag
		val tupleTagG = g.getTupleTag
		val tupleTagH = h.getTupleTag
		val tupleTagI = i.getTupleTag
		val tupleTagJ = j.getTupleTag
		val tupleTagK = k.getTupleTag
		val tupleTagL = l.getTupleTag
		val tupleTagM = m.getTupleTag
		val tupleTagN = n.getTupleTag
		val tupleTagO = o.getTupleTag
		val tupleTagP = p.getTupleTag
		val tupleTagQ = q.getTupleTag
		val tupleTagR = r.getTupleTag
		val tupleTagS = s.getTupleTag
		val tupleTagT = t.getTupleTag
		val tupleTagU = u.getTupleTag
		val tupleTagV = v.getTupleTag
		val fromResult = { (result: CoGbkResult) =>
			(
				result.getAll(tupleTagA).asScala,
				result.getAll(tupleTagB).asScala,
				result.getAll(tupleTagC).asScala,
				result.getAll(tupleTagD).asScala,
				result.getAll(tupleTagE).asScala,
				result.getAll(tupleTagF).asScala,
				result.getAll(tupleTagG).asScala,
				result.getAll(tupleTagH).asScala,
				result.getAll(tupleTagI).asScala,
				result.getAll(tupleTagJ).asScala,
				result.getAll(tupleTagK).asScala,
				result.getAll(tupleTagL).asScala,
				result.getAll(tupleTagM).asScala,
				result.getAll(tupleTagN).asScala,
				result.getAll(tupleTagO).asScala,
				result.getAll(tupleTagP).asScala,
				result.getAll(tupleTagQ).asScala,
				result.getAll(tupleTagR).asScala,
				result.getAll(tupleTagS).asScala,
				result.getAll(tupleTagT).asScala,
				result.getAll(tupleTagU).asScala,
				result.getAll(tupleTagV).asScala
			)
		}
		if (self.isTest) {
			val result = testCoGroup[KEY](a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v)
			val keyed = result.map(kv => kv.getKey -> fromResult(kv.getValue))
			new SortMergeTransform.ReadBuilderTest(self, keyed)
		} else {
			val transform = SortedBucketIO
				.read(keyClass)
				.of(a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v)
				.withTargetParallelism(targetParallelism)
			new SortMergeTransform.ReadBuilderImpl(self, transform, fromResult)
		}
	}

	def sortMergeTransform[KEY: Coder, A: Coder, B: Coder, C: Coder, D: Coder, E: Coder, F: Coder, G: Coder, H: Coder, I: Coder, J: Coder, K: Coder, L: Coder, M: Coder, N: Coder, O: Coder, P: Coder, Q: Coder, R: Coder, S: Coder, T: Coder, U: Coder, V: Coder](keyClass: Class[KEY], a: SortedBucketIO.Read[A], b: SortedBucketIO.Read[B], c: SortedBucketIO.Read[C], d: SortedBucketIO.Read[D], e: SortedBucketIO.Read[E], f: SortedBucketIO.Read[F], g: SortedBucketIO.Read[G], h: SortedBucketIO.Read[H], i: SortedBucketIO.Read[I], j: SortedBucketIO.Read[J], k: SortedBucketIO.Read[K], l: SortedBucketIO.Read[L], m: SortedBucketIO.Read[M], n: SortedBucketIO.Read[N], o: SortedBucketIO.Read[O], p: SortedBucketIO.Read[P], q: SortedBucketIO.Read[Q], r: SortedBucketIO.Read[R], s: SortedBucketIO.Read[S], t: SortedBucketIO.Read[T], u: SortedBucketIO.Read[U], v: SortedBucketIO.Read[V]): SortMergeTransform.ReadBuilder[KEY, KEY, Void, (Iterable[A], Iterable[B], Iterable[C], Iterable[D], Iterable[E], Iterable[F], Iterable[G], Iterable[H], Iterable[I], Iterable[J], Iterable[K], Iterable[L], Iterable[M], Iterable[N], Iterable[O], Iterable[P], Iterable[Q], Iterable[R], Iterable[S], Iterable[T], Iterable[U], Iterable[V])] = {
		sortMergeTransform(keyClass, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, TargetParallelism.auto())
	}


	private[smb] def testCoGroup[K](
		reads: SortedBucketIO.Read[_]*
	): SCollection[KV[K, CoGbkResult]] = {
		val testInput = TestDataManager.getInput(self.testId.get)
		val read :: rs = reads.asInstanceOf[Seq[SortedBucketIO.Read[Any]]].toList: @nowarn
		val test = testInput[(K, Any)](SortedBucketIOUtil.testId(read)).toSCollection(self)
		val keyed = rs
			.foldLeft(KeyedPCollectionTuple.of(read.getTupleTag, test.toKV.internal)) { (kpt, r) =>
				val c = testInput[(K, Any)](SortedBucketIOUtil.testId(r)).toSCollection(self)
				kpt.and(r.getTupleTag, c.toKV.internal)
			}
			.apply(CoGroupByKey.create())
		self.wrap(keyed)
	}
}

object SMBMultiJoin {
	final def apply(sc: ScioContext): SMBMultiJoin = new SMBMultiJoin(sc)
}
