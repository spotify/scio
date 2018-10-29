/*
 * Copyright 2018 Spotify AB.
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

package com.spotify.scio.annotations

/**
 * Public API's (public class, method or field) marked with this annotation is subject
 * to incompatible changes, or even removal, in a future release.
 *
 * <p>Note that the presence of this annotation implies nothing about the quality or performance of
 * the API in question, only the fact that the API or behavior may change in any way.</p>
 */
//scalastyle:off class.name
final class experimental extends scala.annotation.StaticAnnotation
