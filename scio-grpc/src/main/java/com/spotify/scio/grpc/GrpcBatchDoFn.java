/*
 * Copyright 2023 Spotify AB
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

import static java.util.Objects.requireNonNull;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListenableFuture;
import com.spotify.scio.grpc.GrpcDoFn.ChannelSupplier;
import com.spotify.scio.transforms.BaseAsyncLookupDoFn;
import com.spotify.scio.transforms.BaseAsyncLookupDoFn.CacheSupplier;
import com.spotify.scio.transforms.GuavaAsyncBatchLookupDoFn;
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
 *
 * @param <Input> input element type.
 * @param <BatchRequest> batched input element type
 * @param <BatchResponse> batched output element type
 * @param <Output> client lookup value type.
 * @param <Client> client type.
 */
public class GrpcBatchDoFn<
        Input, BatchRequest, BatchResponse, Output, Client extends AbstractFutureStub<Client>>
    extends GuavaAsyncBatchLookupDoFn<Input, BatchRequest, BatchResponse, Output, Client> {
  private final ChannelSupplier channelSupplier;
  private final SerializableFunction<Channel, Client> newClientFn;

  private final SerializableBiFunction<Client, BatchRequest, ListenableFuture<BatchResponse>>
      lookupFn;

  public GrpcBatchDoFn(
      ChannelSupplier channelSupplier,
      SerializableFunction<Channel, Client> newClientFn,
      int batchSize,
      SerializableFunction<List<Input>, BatchRequest> batchRequestFn,
      SerializableFunction<BatchResponse, List<Pair<String, Output>>> batchResponseFn,
      SerializableFunction<Input, String> idExtractorFn,
      SerializableBiFunction<Client, BatchRequest, ListenableFuture<BatchResponse>> lookupFn,
      int maxPendingRequests) {
    super(batchSize, batchRequestFn, batchResponseFn, idExtractorFn, maxPendingRequests);
    this.channelSupplier = channelSupplier;
    this.newClientFn = newClientFn;
    this.lookupFn = lookupFn;
  }

  public GrpcBatchDoFn(
      ChannelSupplier channelSupplier,
      SerializableFunction<Channel, Client> newClientFn,
      int batchSize,
      SerializableFunction<List<Input>, BatchRequest> batchRequestFn,
      SerializableFunction<BatchResponse, List<Pair<String, Output>>> batchResponseFn,
      SerializableFunction<Input, String> idExtractorFn,
      SerializableBiFunction<Client, BatchRequest, ListenableFuture<BatchResponse>> lookupFn,
      int maxPendingRequests,
      CacheSupplier<String, Output> cacheSupplier) {
    super(
        batchSize,
        batchRequestFn,
        batchResponseFn,
        idExtractorFn,
        maxPendingRequests,
        cacheSupplier);
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

  public static <
          Input,
          BatchRequest,
          BatchResponse,
          Output,
          ClientType extends AbstractFutureStub<ClientType>>
      Builder<Input, BatchRequest, BatchResponse, Output, ClientType> newBuilder() {
    return new Builder<>();
  }

  public static class Builder<
          Input,
          BatchRequest,
          BatchResponse,
          Output,
          ClientType extends AbstractFutureStub<ClientType>>
      implements Serializable {

    private ChannelSupplier channelSupplier;
    private SerializableFunction<Channel, ClientType> newClientFn;
    private SerializableBiFunction<ClientType, BatchRequest, ListenableFuture<BatchResponse>>
        lookupFn;
    private SerializableFunction<List<Input>, BatchRequest> batchRequestFn;
    private SerializableFunction<BatchResponse, List<Pair<String, Output>>> batchResponseFn;
    private SerializableFunction<Input, String> idExtractorFn;
    private int maxPendingRequests = GrpcDoFn.DEFAULT_MAX_PENDING_REQUESTS;
    private Integer batchSize;
    private CacheSupplier<String, Output> cacheSupplier =
        new BaseAsyncLookupDoFn.NoOpCacheSupplier<>();

    /**
     * Sets the {@link ChannelSupplier} for creating gRPC channels.
     *
     * @param channelSupplier The {@link ChannelSupplier} to use for creating gRPC channels.
     * @return The updated {@link Builder} instance.
     */
    public Builder<Input, BatchRequest, BatchResponse, Output, ClientType> withChannelSupplier(
        ChannelSupplier channelSupplier) {
      this.channelSupplier = channelSupplier;
      return this;
    }

    /**
     * Sets a new client function. This method takes a {@link SerializableFunction} that creates a
     * gRPC async stub of type {@code <ClientType>} from the provided {@link Channel}. The new
     * client function will be used to create the client for making gRPC requests.
     *
     * @param newClientFn The {@link SerializableFunction} that creates the gRPC async stub.
     * @return The updated {@link Builder} instance.
     */
    public Builder<Input, BatchRequest, BatchResponse, Output, ClientType> withNewClientFn(
        SerializableFunction<Channel, ClientType> newClientFn) {
      this.newClientFn = newClientFn;
      return this;
    }

    /**
     * Sets the lookup function to be used for performing batch requests. This provided {@link
     * SerializableBiFunction} should take a gRPC {@code <ClientType>} and a {@code <BatchRequest>}
     * as input and returns a {@link ListenableFuture} of a {@code <BatchResponse>}.
     *
     * @param lookupFn The lookup function to be used for performing batch requests.
     * @return The updated {@link Builder} instance.
     */
    public Builder<Input, BatchRequest, BatchResponse, Output, ClientType> withLookupFn(
        SerializableBiFunction<ClientType, BatchRequest, ListenableFuture<BatchResponse>>
            lookupFn) {
      this.lookupFn = lookupFn;
      return this;
    }

    /**
     * Sets the batch request function. This method takes a {@link SerializableFunction} that takes
     * a {@link List} of {@code <Input>} objects representing the elements that will go into the
     * {@code <BatchRequest>}. The {@link SerializableFunction} should return the {@code
     * <BatchRequest>} that will be sent via gRPC.
     *
     * @param batchRequestFn The batch request function.
     * @return The updated {@link Builder} instance.
     */
    public Builder<Input, BatchRequest, BatchResponse, Output, ClientType> withBatchRequestFn(
        SerializableFunction<List<Input>, BatchRequest> batchRequestFn) {
      this.batchRequestFn = batchRequestFn;
      return this;
    }

    /**
     * Sets the batch response function.
     * The batch response function is a {@link SerializableFunction} that takes the
     * {@code <BatchResponse>} coming from the gRPC endpoint and returns a {@link List} of a
     * {@link Pair} containing the ID of the {@code <Input>} and the corresponding {@code <Output>}.
     * The ID returned by the function must match the ID of the {@code <Input>} that resulted in
     * that {@code <Output>.
     * If the ID returned does not match any {@code <Input>} ID, the pipeline will fail.
     *
     * @param batchResponseFn The batch response function to be set.
     * @return The updated {@link Builder} instance.
     */
    public Builder<Input, BatchRequest, BatchResponse, Output, ClientType> withBatchResponseFn(
        SerializableFunction<BatchResponse, List<Pair<String, Output>>> batchResponseFn) {
      this.batchResponseFn = batchResponseFn;
      return this;
    }

    /**
     * Sets the ID extractor function. The ID extractor function is a {@link SerializableFunction}
     * that takes a single {@code <Input>} as a parameter and returns a String. The returned {@link
     * String} represents a unique ID identifying the {@code <Input>}. This ID is used to match the
     * {@code <Output>} inside the {@code <BatchResponse>}. Additionally, it is passed to the {@link
     * com.spotify.scio.util.Cache} as the key to if a {@link CacheSupplier} is provided.
     *
     * @param idExtractorFn the ID extractor function to set
     * @return The updated {@link Builder} instance.
     */
    public Builder<Input, BatchRequest, BatchResponse, Output, ClientType> withIdExtractorFn(
        SerializableFunction<Input, String> idExtractorFn) {
      this.idExtractorFn = idExtractorFn;
      return this;
    }

    /**
     * Sets the maximum number of pending requests allowed. This number represents the maximum
     * number of parallel batch requests that can be created per DoFn instance.
     *
     * @param maxPendingRequests The maximum number of pending requests for the batch processing.
     * @return The updated {@link Builder} instance.
     */
    public Builder<Input, BatchRequest, BatchResponse, Output, ClientType> withMaxPendingRequests(
        int maxPendingRequests) {
      Preconditions.checkArgument(maxPendingRequests > 0, "maxPendingRequests must be positive");
      this.maxPendingRequests = maxPendingRequests;
      return this;
    }

    /**
     * Sets the batch size for batching elements. The batch size determines the maximum number of
     * elements that can be batched into a single {@code <BatchRequest>}. Batches are created from
     * the bundle elements, and we do not batch across bundles.
     *
     * @param batchSize The batch size to set.
     * @return The updated {@link Builder} instance.
     */
    public Builder<Input, BatchRequest, BatchResponse, Output, ClientType> withBatchSize(
        int batchSize) {
      Preconditions.checkArgument(batchSize > 0, "batchSize must be positive");
      this.batchSize = batchSize;
      return this;
    }

    /**
     * Sets the cache supplier for the Builder. This method allows you to set a {@link
     * CacheSupplier} that is capable of supplying a {@link com.spotify.scio.util.Cache} of type
     * {@link String} and {@code <Output>}. Where the {@link String} is the ID returned from the
     * IdExtractorFn and is matched to a specific {@code <Output>} from the {@code <BatchResponse>}.
     *
     * @param cacheSupplier the {@link CacheSupplier} to set for the Builder
     * @return The updated {@link Builder} instance.
     */
    public Builder<Input, BatchRequest, BatchResponse, Output, ClientType> withCacheSupplier(
        CacheSupplier<String, Output> cacheSupplier) {
      this.cacheSupplier = cacheSupplier;
      return this;
    }

    public GrpcBatchDoFn<Input, BatchRequest, BatchResponse, Output, ClientType> build() {
      requireNonNull(channelSupplier, "channelSupplier must not be null");
      requireNonNull(newClientFn, "newClientFn must not be null");
      requireNonNull(lookupFn, "lookupFn must not be null");
      requireNonNull(batchRequestFn, "batchRequestFn must not be null");
      requireNonNull(batchResponseFn, "batchResponseFn must not be null");
      requireNonNull(idExtractorFn, "idExtractorFn must not be null");
      requireNonNull(batchSize, "batchSize must not be null");
      requireNonNull(cacheSupplier, "cacheSupplier must not be null");

      return new GrpcBatchDoFn<>(
          channelSupplier,
          newClientFn,
          batchSize,
          batchRequestFn,
          batchResponseFn,
          idExtractorFn,
          lookupFn,
          maxPendingRequests,
          cacheSupplier);
    }
  }
}
