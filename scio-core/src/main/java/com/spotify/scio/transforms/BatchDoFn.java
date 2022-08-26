package com.spotify.scio.transforms;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Batches input into a desired batch size.
 *
 * <p>Elements are buffered until there are enough elements for a batch,
 * at which point they are emitted to the output PCollection
 *
 * <p>Windows are preserved (batches contain elements from the same window).
 * Batches are not spanning over bundles. Once a bundle is finished, the batch is emitted even if not full.
 * This function can only batch 10 parallel windows. If new element comes from an 11th window,
 * the bigger batch will be emitted to give room for this new element.
 *
 * @param <InputT>
 */
public class BatchDoFn<InputT> extends DoFn<InputT, Iterable<InputT>> {

    private static final Logger LOG = LoggerFactory.getLogger(BatchDoFn.class);

    public static final int DEFAULT_MAX_LIVE_WINDOWS = 10;

    private static class Buffer<InputT> {

        private final List<InputT> elements;

        private long weight;

        Buffer() {
            this.elements = new ArrayList<>();
            this.weight = 0;
        }

        public List<InputT> getElements() {
            return elements;
        }

        public long getWeight() {
            return weight;
        }

        public void add(InputT element, long weight) {
            elements.add(element);
            this.weight += weight;
        }
    }

    private final long maxWeight;
    private final SerializableFunction<InputT, Long> weigher;

    private final int maxLiveWindows;
    private transient Map<BoundedWindow, Buffer<InputT>> buffers;

    public BatchDoFn(
            long maxWeight,
            SerializableFunction<InputT, Long> weigher
    ) { this(maxWeight, weigher, DEFAULT_MAX_LIVE_WINDOWS); }

    public BatchDoFn(
            long maxWeight,
            SerializableFunction<InputT, Long> weigher,
            int maxLiveWindows
    ) {
        this.maxWeight = maxWeight;
        this.weigher = weigher;
        this.maxLiveWindows = maxLiveWindows;
    }

    @Setup
    public void setup() {
        this.buffers = new HashMap<>();
    }

    @ProcessElement
    public void processElement(
            @Element InputT element,
            BoundedWindow window,
            OutputReceiver<Iterable<InputT>> receiver
    ) {
        LOG.debug("*** BATCH *** Add element for window {} ", window);
        Buffer<InputT> buffer = buffers.computeIfAbsent(window, w -> new Buffer<>());
        long weight = weigher.apply(element);
        buffer.add(element, weight);

        if (buffer.getWeight() >= maxWeight) {
            LOG.debug("*** END OF BATCH *** for window {}", window);
            flushBatch(window, receiver);
        } else if (buffers.size() > maxLiveWindows) {
            // flush the biggest buffer if we get too many parallel windows
            BoundedWindow discardedWindow = Collections
                    .max(buffers.entrySet(), Comparator.comparingLong(o -> o.getValue().getWeight()))
                    .getKey();
            LOG.debug("*** END OF BATCH *** for window {}", discardedWindow);
            flushBatch(discardedWindow, receiver);
        }
    }

    @FinishBundle
    public void finishBundle(FinishBundleContext context) {
        for (Map.Entry<BoundedWindow, Buffer<InputT>> wb : buffers.entrySet()) {
            BoundedWindow window = wb.getKey();
            Buffer<InputT> buffer = wb.getValue();
            context.output(buffer.getElements(), window.maxTimestamp(), window);
        }
        buffers.clear();
    }

    private void flushBatch(
            BoundedWindow window,
            OutputReceiver<Iterable<InputT>> receiver
    ) {
        // prefer removing the window than clearing the buffer
        // to avoid reaching MAX_LIVE_WINDOWS if no other element
        // come for this window
        Buffer<InputT> buffer = buffers.remove(window);
        // Set the batch timestamp to the window's maxTimestamp
        receiver.outputWithTimestamp(buffer.elements, window.maxTimestamp());
    }

}
