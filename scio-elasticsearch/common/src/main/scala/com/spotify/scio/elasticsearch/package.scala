/*
 * Copyright 2022 Spotify AB.
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

package com.spotify.scio

import com.spotify.scio.elasticsearch.syntax.SCollectionSyntax

/**
 * Main package for Elasticsearch APIs. Import all.
 *
 * {{{
 * import com.spotify.scio.elasticsearch._
 * }}}
 */
package object elasticsearch extends SCollectionSyntax with CoderInstances {

  @deprecated("Use syntax.ElasticsearchSCollectionOps instead", "0.14.0")
  type ElasticsearchSCollection[T] = syntax.ElasticsearchSCollectionOps[T]
}
