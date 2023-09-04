package com.spotify.scio.grpc;

import static java.util.Objects.requireNonNull;

import com.google.common.util.concurrent.ListenableFuture;
import com.spotify.scio.grpc.GrpcDoFn.ChannelSupplier;
import com.spotify.scio.transforms.BaseAsyncLookupDoFn.CacheSupplier;
import com.spotify.scio.transforms.BatchedGuavaAsyncLookupDoFn;
import com.spotify.scio.transforms.DoFnWithResource.ResourceType;
import io.grpc.Channel;
import io.grpc.stub.AbstractFutureStub;
import java.io.Serializable;
import java.util.List;
import org.apache.beam.sdk.transforms.SerializableBiFunction;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.commons.lang3.tuple.Pair;

/**
 * DoFn that makes API calls that can be batched and individually cached over a managed GRPC
 * channel.
 * @param <Input>         input element type.
 * @param <BatchRequest>  batched input element type
 * @param <BatchResponse> batched output element type
 * @param <Output>        client lookup value type.
 * @param <Client>    client type.
 */
public class BatchedGrpcDoFn<Input, BatchRequest, BatchResponse, Output, Client extends AbstractFutureStub<Client>> extends
    BatchedGuavaAsyncLookupDoFn<Input, BatchRequest, BatchResponse, Output, Client> {

  private final ChannelSupplier channelSupplier;
  private final SerializableFunction<Channel, Client> newClientFn;

  private final SerializableBiFunction<Client, BatchRequest, ListenableFuture<BatchResponse>> lookupFn;

  public BatchedGrpcDoFn(
      ChannelSupplier channelSupplier,
      SerializableFunction<Channel, Client> newClientFn,
      SerializableBiFunction<Client, BatchRequest, ListenableFuture<BatchResponse>> lookupFn,
      SerializableFunction<List<Input>, BatchRequest> batchRequestFn,
      SerializableFunction<BatchResponse, List<Pair<String, Output>>> batchResponseFn,
      SerializableFunction<Input, String> idExtractorFn,
      Integer maxPendingRequests,
      Integer batchSize,
      CacheSupplier<String, Output> cacheSupplier
  ) {
    super(batchSize, maxPendingRequests, cacheSupplier, batchRequestFn, batchResponseFn,
        idExtractorFn);
    this.channelSupplier = channelSupplier;
    this.newClientFn = newClientFn;
    this.lookupFn = lookupFn;
  }

  @Override
  public ResourceType getResourceType() {
    return ResourceType.PER_INSTANCE;
  }

  @Override
  public ListenableFuture<BatchResponse> asyncLookup(Client client, BatchRequest request) {
    return lookupFn.apply(client, request);
  }

  @Override
  protected Client newClient() {
    return newClientFn.apply(channelSupplier.get());
  }

  public static <Input, BatchRequest, BatchResponse, Output, ClientType
      extends AbstractFutureStub<ClientType>>
  Builder<Input, BatchRequest, BatchResponse, Output, ClientType> newBuilder() {
    return new Builder<>();
  }

  public static class Builder<Input, BatchRequest, BatchResponse, Output, ClientType extends AbstractFutureStub<ClientType>> implements
      Serializable {

    private ChannelSupplier channelSupplier;
    private SerializableFunction<Channel, ClientType> newClientFn;
    private SerializableBiFunction<ClientType, BatchRequest, ListenableFuture<BatchResponse>> lookupFn;
    private SerializableFunction<List<Input>, BatchRequest> batchRequestFn;
    private SerializableFunction<BatchResponse, List<Pair<String, Output>>> batchResponseFn;
    private SerializableFunction<Input, String> idExtractionFn;
    private Integer maxPendingRequests;
    private Integer batchSize;
    private CacheSupplier<String, Output> cacheSupplier;

    public Builder<Input, BatchRequest, BatchResponse, Output, ClientType> withChannelSupplier(
        ChannelSupplier channelSupplier
    ) {
      this.channelSupplier = channelSupplier;
      return this;
    }

    public Builder<Input, BatchRequest, BatchResponse, Output, ClientType> withNewClientFn(
        SerializableFunction<Channel, ClientType> newClientFn
    ) {
      this.newClientFn = newClientFn;
      return this;
    }

    public Builder<Input, BatchRequest, BatchResponse, Output, ClientType> withLookupFn(
        SerializableBiFunction<ClientType, BatchRequest, ListenableFuture<BatchResponse>> lookupFn
    ) {
      this.lookupFn = lookupFn;
      return this;
    }

    public Builder<Input, BatchRequest, BatchResponse, Output, ClientType> withBatchRequestFn(
        SerializableFunction<List<Input>, BatchRequest> batchRequestFn
    ) {
      this.batchRequestFn = batchRequestFn;
      return this;
    }

    public Builder<Input, BatchRequest, BatchResponse, Output, ClientType> withBatchResponseFn(
        SerializableFunction<BatchResponse, List<Pair<String, Output>>> batchResponseFn
    ) {
      this.batchResponseFn = batchResponseFn;
      return this;
    }

    public Builder<Input, BatchRequest, BatchResponse, Output, ClientType> withIdExtractionFn(
        SerializableFunction<Input, String> idExtractionFn
    ) {
      this.idExtractionFn = idExtractionFn;
      return this;
    }

    public Builder<Input, BatchRequest, BatchResponse, Output, ClientType> withMaxPendingRequests(
        Integer maxPendingRequests
    ) {
      this.maxPendingRequests = maxPendingRequests;
      return this;
    }

    public Builder<Input, BatchRequest, BatchResponse, Output, ClientType> withBatchSize(
        Integer batchSize
    ) {
      this.batchSize = batchSize;
      return this;
    }

    public Builder<Input, BatchRequest, BatchResponse, Output, ClientType> withCacheSupplier(
        CacheSupplier<String, Output> cacheSupplier
    ) {
      this.cacheSupplier = cacheSupplier;
      return this;
    }

    public BatchedGrpcDoFn<Input, BatchRequest, BatchResponse, Output, ClientType> build() {
      requireNonNull(channelSupplier, "channelSupplier must not be null");
      requireNonNull(newClientFn, "newClientFn must not be null");
      requireNonNull(lookupFn, "lookupFn must not be null");
      requireNonNull(batchRequestFn, "batchRequestFn must not be null");
      requireNonNull(batchResponseFn, "batchResponseFn must not be null");
      requireNonNull(idExtractionFn, "idExtractionFn must not be null");
      requireNonNull(maxPendingRequests, "maxPendingRequests must not be null");
      requireNonNull(batchSize, "batchSize must not be null");
      requireNonNull(cacheSupplier, "cacheSupplier must not be null");

      return new BatchedGrpcDoFn<>(
          channelSupplier,
          newClientFn,
          lookupFn,
          batchRequestFn,
          batchResponseFn,
          idExtractionFn,
          maxPendingRequests,
          batchSize,
          cacheSupplier
      );
    }
  }
}