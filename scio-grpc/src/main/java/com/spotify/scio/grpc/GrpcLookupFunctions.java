/*
 * Copyright 2022 Spotify AB
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

package com.spotify.scio.grpc;

import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.stub.StreamObserver;
import java.io.Serializable;
import java.util.LinkedList;

/** Defines lookup functions specific to the GRPC Java library. */
public class GrpcLookupFunctions {

  // An interface between GRPC's StreamObservable and a Guava ListenableFuture
  static class StreamObservableFuture<ResponseT> extends AbstractFuture<Iterable<ResponseT>>
      implements StreamObserver<ResponseT> {
    private final LinkedList<ResponseT> responses;

    StreamObservableFuture() {
      responses = new LinkedList<>();
    }

    @Override
    public void onNext(ResponseT value) {
      responses.add(value);
    }

    @Override
    public void onError(Throwable t) {
      setException(t);
    }

    @Override
    public void onCompleted() {
      // Set the value of the AbstractFuture
      set(responses);
    }
  }
}
