package com.spotify.scio.grpc;

import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.stub.StreamObserver;
import java.io.Serializable;
import java.util.LinkedList;

/**
 * Defines lookup functions specific to the GRPC Java library.
 */
public class GrpcLookupFunctions {


    // An interface between GRPC's StreamObservable and a Guava ListenableFuture
    static class StreamObservableFuture<ResponseT>
            extends AbstractFuture<Iterable<ResponseT>> implements StreamObserver<ResponseT> {
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