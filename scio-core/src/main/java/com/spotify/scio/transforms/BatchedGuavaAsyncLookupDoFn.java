package com.spotify.scio.transforms;

import com.google.common.util.concurrent.ListenableFuture;
import com.spotify.scio.transforms.BaseAsyncLookupDoFn.CacheSupplier;
import com.spotify.scio.transforms.BaseAsyncLookupDoFn.Try;
import java.util.List;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.commons.lang3.tuple.Pair;

/**
 * @param <Input>         input element type.
 * @param <BatchRequest>  batched input element type
 * @param <BatchResponse> batched output element type
 * @param <Output>        client lookup value type.
 * @param <ClientType>    client type.
 */
public abstract class BatchedGuavaAsyncLookupDoFn<Input, BatchRequest, BatchResponse, Output, ClientType> extends
    BatchedBaseAsyncLookupDoFn<Input, BatchRequest, BatchResponse, Output, ClientType, ListenableFuture<BatchResponse>, Try<Output>> implements
    FutureHandlers.Guava<BatchResponse> {

  public BatchedGuavaAsyncLookupDoFn(int batchSize,
      int maxPendingRequests,
      CacheSupplier<String, Output> cacheSupplier,
      SerializableFunction<List<Input>, BatchRequest> batchRequestFn,
      SerializableFunction<BatchResponse, List<Pair<String, Output>>> batchResponseFn,
      SerializableFunction<Input, String> idExtractorFn) {
    super(batchSize, maxPendingRequests, cacheSupplier, batchRequestFn, batchResponseFn,
        idExtractorFn);
  }

  @Override
  public Try<Output> success(Output output) {
    return new Try<>(output);
  }

  @Override
  public Try<Output> failure(Throwable throwable) {
    return new Try<>(throwable);
  }
}
