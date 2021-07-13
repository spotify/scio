package org.apache.beam.sdk.extensions.smb;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGbkResultSchema;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Function;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterators;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.primitives.UnsignedBytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("unchecked")
public class MultiSourceKeyGroupReader<FinalKeyT> {
  private static final Logger LOG = LoggerFactory.getLogger(MultiSourceKeyGroupReader.class);

  private enum AcceptKeyGroup { ACCEPT, REJECT, UNSET }
  private Optional<KV<FinalKeyT, CoGbkResult>> head = null;

  private final Coder<FinalKeyT> keyCoder;

  private int runningKeyGroupSize = 0;
  private final Distribution keyGroupSize;
  private final boolean materializeKeyGroup;
  private final Comparator<byte[]> bytesComparator = UnsignedBytes.lexicographicalComparator();

  private final CoGbkResultSchema resultSchema;
  private final List<BucketedInputSource<?, ?>> bucketedInputs;
  private final Function<byte[], Boolean> keyGroupFilter;

  public MultiSourceKeyGroupReader(
      List<SortedBucketSource.BucketedInput<?, ?>> sources,
       SourceSpec<FinalKeyT> sourceSpec,
       Distribution keyGroupSize,
       boolean materializeKeyGroup,
      int bucketId,
      int effectiveParallelism,
      PipelineOptions options
  ) {
    this.keyCoder = sourceSpec.keyCoder;
    this.keyGroupSize = keyGroupSize;
    this.materializeKeyGroup = materializeKeyGroup;

    // source TupleTags `and`-ed together
    this.resultSchema = SortedBucketSource.BucketedInput.schemaOf(sources);
    this.bucketedInputs =
        sources.stream()
            .map(src -> new BucketedInputSource<>(src, bucketId, effectiveParallelism, options))
            .collect(Collectors.toList());
    this.keyGroupFilter = (bytes) -> sources.get(0).getMetadata().rehashBucket(bytes, effectiveParallelism) == bucketId;
  }

  public Optional<KV<FinalKeyT, CoGbkResult>> readNext() {
    advance();
    return head;
  }

  private void advance() {
    // once all sources are exhausted, head is empty, so short circuit return
    if(head != null && !head.isPresent()) return;

    while(true) {
      if (runningKeyGroupSize != 0) {
        keyGroupSize.update(runningKeyGroupSize);
        runningKeyGroupSize = 0;
      }

      // advance iterators whose values have already been used.
      bucketedInputs.stream()
          .filter(src -> !src.isExhausted())
          .filter(src -> !src.currentValue().hasNext())
          .forEach(src -> src.advance());

      // only operate on the non-exhausted sources
      List<BucketedInputSource<?, ?>> activeSources = bucketedInputs.stream()
              .filter(src -> !src.isExhausted())
              .collect(Collectors.toList());

      // once all sources are exhausted, set head to empty and return
      if(activeSources.isEmpty()) {
        head = Optional.empty();
        break;
      }

      // process keys in order, but since not all sources have all keys, find the minimum available key
      final List<byte[]> consideredKeys = activeSources.stream().map(src -> src.currentKey()).collect(Collectors.toList());
      byte[] minKey = consideredKeys.stream().min(bytesComparator).orElse(null);
      final boolean emitBasedOnMinKeyBucketing = keyGroupFilter.apply(minKey);

      // output accumulator
      final List<Iterable<?>> valueMap = IntStream.range(0, resultSchema.size()).mapToObj(i -> new ArrayList<>()).collect(Collectors.toList());

      // minKey will be accepted or rejected by the first source which has it.
      // acceptKeyGroup short-circuits the 'emit' logic below once a decision is made on minKey.
      AcceptKeyGroup acceptKeyGroup = AcceptKeyGroup.UNSET;
      for(BucketedInputSource<?, ?> src : activeSources) {
        // for each source, if the current key is equal to the minimum, consume it
        if(!src.isExhausted() && bytesComparator.compare(minKey, src.currentKey()) == 0) {
          // if this key group has been previously accepted by a preceding source, emit.
          // "  "    "   "     "   "    "          rejected by a preceding source, don't emit.
          // if this is the first source for this key group, emit if either the source settings or the min key say we should.
          boolean emitKeyGroup = (acceptKeyGroup == AcceptKeyGroup.ACCEPT) ||
                                 ((acceptKeyGroup == AcceptKeyGroup.UNSET) && (src.emitByDefault || emitBasedOnMinKeyBucketing));

          final Iterator<Object> keyGroupIterator = (Iterator<Object>) src.currentValue();
          if(emitKeyGroup) {
            acceptKeyGroup = AcceptKeyGroup.ACCEPT;
            // data must be eagerly materialized if requested or if there is a predicate
            boolean materialize = materializeKeyGroup || src.predicate.isPresent();
            int outputIndex = resultSchema.getIndex(src.tupleTag);

            if(!materialize) {
              // lazy data iterator
              valueMap.set(
                  outputIndex,
                  new SortedBucketSource.TraversableOnceIterable<>(
                      Iterators.transform(
                          keyGroupIterator, (value) -> {
                            runningKeyGroupSize++;
                            return value;
                          }
                      )
                  )
              );

            } else {
              // eagerly materialize this iterator and apply the predicate to each value
              // this must be eager because the predicate can operate on the entire collection
              final List<Object> values = (List<Object>) valueMap.get(outputIndex);
              final SortedBucketSource.Predicate<Object> predicate= src.predicate
                  .map(value -> (SortedBucketSource.Predicate<Object>) value)
                  .orElseGet(() -> (xs, x) -> true);
              keyGroupIterator.forEachRemaining(v -> {
                if ((predicate).apply(values, v)) {
                  values.add(v);
                  runningKeyGroupSize++;
                }
              });
            }
          } else {
            acceptKeyGroup = AcceptKeyGroup.REJECT;
            // skip key but still have to exhaust iterator
            keyGroupIterator.forEachRemaining(value -> {});
          }
        }
      }

      if(acceptKeyGroup == AcceptKeyGroup.ACCEPT) {
        final KV<byte[], CoGbkResult> next = KV.of(minKey, CoGbkResultUtil.newCoGbkResult(resultSchema, valueMap));
        try {
          // new head found, we're done
          head = Optional.of(KV.of(keyCoder.decode(new ByteArrayInputStream(next.getKey())), next.getValue()));
          break;
        } catch (Exception e) {
          throw new RuntimeException("Failed to decode key group", e);
        }
      }
    }
  }

  private static class BucketedInputSource<K, V> {
    public final TupleTag<?> tupleTag;
    public final boolean emitByDefault;

    private final KeyGroupIterator<byte[], V> iter;
    final Optional<SortedBucketSource.Predicate<V>> predicate;
    private Optional<KV<byte[], Iterator<V>>> head;

    public BucketedInputSource(
        SortedBucketSource.BucketedInput<K, V>  source,
        int bucketId,
        int parallelism,
        PipelineOptions options
    ) {
      this.predicate = Optional.ofNullable(source.getPredicate());
      this.tupleTag = source.getTupleTag();
      this.iter = source.createIterator(bucketId, parallelism, options);

      int numBuckets = source.getMetadata().getNumBuckets();
      // The canonical # buckets for this source. If # buckets >= the parallelism of the job,
      // we know that the key doesn't need to be re-hashed as it's already in the right bucket.
      this.emitByDefault = numBuckets >= parallelism;

      advance();
    }

    public byte[] currentKey() {
      return head.get().getKey();
    }

    public Iterator<V> currentValue() {
      return head.get().getValue();
    }

    public boolean isExhausted() {
      return !head.isPresent();
    }

    public void advance() {
      if(iter.hasNext()) {
        head = Optional.of(iter.next());
      } else {
        head = Optional.empty();
      }
    }
  }
}
