package com.spotify.scio.grpc;

import com.google.common.util.concurrent.ListenableFuture;
import com.spotify.scio.transforms.GuavaAsyncLookupDoFn;
import io.grpc.Channel;
import io.grpc.stub.AbstractStub;
import org.apache.beam.sdk.transforms.SerializableBiFunction;
import org.apache.beam.sdk.transforms.SerializableFunction;

import java.io.Serializable;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * DoFn that makes API calls over a managed GRPC channel.
 *
 * @param <RequestT>
 * @param <ResponseT>
 * @param <ClientT>
 */
public class GrpcDoFn<RequestT, ResponseT, ClientT extends AbstractStub<ClientT>>
        extends GuavaAsyncLookupDoFn<RequestT, ResponseT, ClientT> {
    static int DEFAULT_MAX_PENDING_REQUESTS = 1000;

    private final ChannelSupplier channelSupplier;
    private final SerializableFunction<Channel, ClientT> newClientFn;
    private final SerializableBiFunction<ClientT, RequestT, ListenableFuture<ResponseT>> lookupFn;

    GrpcDoFn(
            ChannelSupplier channelSupplier,
            SerializableFunction<Channel, ClientT> newClientFn,
            SerializableBiFunction<ClientT, RequestT, ListenableFuture<ResponseT>> lookupFn,
            Integer maxPendingRequests
    ) {
        super(maxPendingRequests, new NoOpCacheSupplier<>());
        this.channelSupplier = channelSupplier;
        this.newClientFn = newClientFn;
        this.lookupFn = lookupFn;
    }

    @Override
    public ListenableFuture<ResponseT> asyncLookup(ClientT client, RequestT request) {
        return lookupFn.apply(client, request);
    }

    @Override
    protected ClientT newClient() {
        return newClientFn.apply(channelSupplier.get());
    }

    @Override
    public Try<ResponseT> failure(Throwable throwable) {
        return new Try<>(throwable);
    }

    public static <RequestT, ResponseT, ClientT extends AbstractStub<ClientT>> Builder<RequestT, ResponseT, ClientT> newBuilder() {
        return new Builder<>();
    }

    @FunctionalInterface
    public interface ChannelSupplier extends Serializable, Supplier<Channel> {}

    public static class Builder<RequestT, ResponseT, ClientT extends AbstractStub<ClientT>> implements Serializable {

        private int maxPendingRequests = DEFAULT_MAX_PENDING_REQUESTS;
        private ChannelSupplier channelSupplier;
        private SerializableFunction<Channel, ClientT> newClientFn;
        private SerializableBiFunction<ClientT, RequestT, ListenableFuture<ResponseT>> lookupFn;

        protected Builder() {
        }

        public Builder<RequestT, ResponseT, ClientT> withChannelSupplier(ChannelSupplier channelSupplier) {
            this.channelSupplier = channelSupplier;
            return this;
        }

        public Builder<RequestT, ResponseT, ClientT> withNewClientFn(
                SerializableFunction<Channel, ClientT> newClientFn) {
            this.newClientFn = newClientFn;
            return this;
        }

        public Builder<RequestT, ResponseT, ClientT> withLookupFn(
                SerializableBiFunction<ClientT, RequestT, ListenableFuture<ResponseT>> lookupFn) {
            this.lookupFn = lookupFn;
            return this;
        }

        public Builder<RequestT, ResponseT, ClientT> withMaxPendingRequests(int maxPendingRequests) {
            this.maxPendingRequests = maxPendingRequests;
            return this;
        }

        public GrpcDoFn<RequestT, ResponseT, ClientT> build() {
            requireNonNull(channelSupplier, "channelSupplier cannot be null");
            requireNonNull(lookupFn, "lookupFn cannot be null");
            requireNonNull(newClientFn, "newClientFn cannot be null");

            return new GrpcDoFn<>(
                    channelSupplier,
                    newClientFn,
                    lookupFn,
                    maxPendingRequests
            );
        }
    }
}