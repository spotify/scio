/*
 * Copyright 2017 Spotify AB.
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

package com.spotify.scio.transforms;

import org.apache.beam.sdk.transforms.DoFn;

import java.util.concurrent.CompletableFuture;

/**
 * A {@link DoFn} that handles asynchronous requests to an external service that returns Java 8
 * {@link CompletableFuture}s.
 */
public abstract class JavaAsyncDoFn<InputT, OutputT, ResourceT>
    extends BaseAsyncDoFn<InputT, OutputT, ResourceT, CompletableFuture<OutputT>>
    implements FutureHandlers.Java<OutputT> {}
