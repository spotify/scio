/*
 * Copyright 2020 Spotify AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.spotify.scio.transforms.syntax

import com.spotify.scio.values.SCollection
import com.spotify.scio.transforms.DoFnWithResource.ResourceType
import com.spotify.scio.coders.Coder
import com.spotify.scio.transforms.{
  CollectFnWithResource,
  FilterFnWithResource,
  FlatMapFnWithResource,
  MapFnWithResource
}

trait SCollectionWithResourceSyntax {

  implicit class SCollectionWithResourceFunctions[T](private val self: SCollection[T]) {

    /**
     * Return a new [[SCollection]] by applying a function that also takes in a resource and
     * `ResourceType` to all elements of this SCollection.
     */
    def mapWithResource[R, U: Coder](resource: => R, resourceType: ResourceType)(
      fn: (R, T) => U
    ): SCollection[U] =
      self.parDo(new MapFnWithResource(resource, resourceType, fn))

    /**
     * Filter the elements for which the given `PartialFunction` that also takes in a resource and
     * `ResourceType` is defined, and then map.
     */
    def collectWithResource[R, U: Coder](resource: => R, resourceType: ResourceType)(
      pfn: PartialFunction[(R, T), U]
    ): SCollection[U] =
      self.parDo(new CollectFnWithResource(resource, resourceType, pfn))

    /**
     * Return a new [[SCollection]] by first applying a function that also takes in a resource and
     * `ResourceType` to all elements of this SCollection, and then flattening the results.
     */
    def flatMapWithResource[R, U: Coder](resource: => R, resourceType: ResourceType)(
      fn: (R, T) => TraversableOnce[U]
    ): SCollection[U] =
      self.parDo(new FlatMapFnWithResource(resource, resourceType, fn))

    /**
     * Return a new [[SCollection]] containing only the elements that satisfy a predicate that
     * takes in a resource and `ResourceType`
     */
    def filterWithResource[R](resource: => R, resourceType: ResourceType)(
      fn: (R, T) => Boolean
    ): SCollection[T] =
      self.parDo(new FilterFnWithResource(resource, resourceType, fn))(self.coder)

  }
}
