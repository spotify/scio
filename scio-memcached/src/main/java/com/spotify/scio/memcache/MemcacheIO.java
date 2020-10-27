package com.spotify.scio.memcache;

import com.google.auto.value.AutoValue;
import com.spotify.folsom.MemcacheClient;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

public class MemcacheIO {
//    TODO: Right now only with connection configuration is added, we might want to add more withMethods

    private MemcacheIO() {
    }

    public static Write write() {
        return new AutoValue_MemcacheIO_Write.Builder().build();
    }


    @AutoValue
    public abstract static class Write extends PTransform<PCollection<KV<String, String>>, PDone> {

        abstract @Nullable
        MemcacheConnectionConfiguration memcacheConnectionConfiguration();

        abstract Builder toBuilder();

        @AutoValue.Builder
        abstract static class Builder {
            abstract Builder setMemcacheConnectionConfiguration(MemcacheConnectionConfiguration memcacheConnectionConfiguration);

            abstract Write build();
        }

        public Write withMemcacheConnectionConfiguration(MemcacheConnectionConfiguration memcacheConnectionConfiguration) {
            return toBuilder().setMemcacheConnectionConfiguration(memcacheConnectionConfiguration).build();
        }

        @Override
        public PDone expand(PCollection<KV<String, String>> input) {
            input.apply(ParDo.of(new MemcacheWriterFn(this)));
            return PDone.in(input.getPipeline());

        }


        static class MemcacheWriterFn extends DoFn<KV<String, String>, Void> {

            private final Write spec;
            private transient MemcacheClient<String> memcacheClient;

            MemcacheWriterFn(final Write spec) {
                this.spec = spec;
            }

            @Setup
            public void setup() {
                memcacheClient = Objects.requireNonNull(spec.memcacheConnectionConfiguration()).connect();
            }

            @StartBundle
            public void startBundle() {
//                One of them can work, have to find out if waiting at start of bundle is a good idea
//                memwcacheClient.awaitFullyConnected();
//                memcacheClient.connectFuture()
            }

            @ProcessElement
            public void processElement(ProcessContext processContext) {
//                Insted
                KV<String, String> record = processContext.element();
                memcacheClient.add(record.getKey(), record.getValue(), Objects.requireNonNull(spec.memcacheConnectionConfiguration()).ttl());
//                check the status and then of fail thrio
            }

            @FinishBundle
            public void finishBundle() {
                memcacheClient.flushAll(Objects.requireNonNull(spec.memcacheConnectionConfiguration()).flushDelay());
            }

            @Teardown
            public void tearDown() {
                memcacheClient.flushAll(Objects.requireNonNull(spec.memcacheConnectionConfiguration()).flushDelay());
                memcacheClient.shutdown();
            }
        }
    }

}
