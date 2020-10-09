package com.spotify.scio.memcache;

import com.google.auto.value.AutoValue;
import com.spotify.folsom.MemcacheClient;
import com.spotify.folsom.MemcacheClientBuilder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

public class MemcacheIO {

  private MemcacheIO() {}

  public static Write write() {
    return new AutoValue_MemcacheIO_Write.Builder().build();
  }


  @AutoValue
  public abstract static class Write extends PTransform<PCollection<KV<String, String>>, PDone> {

    @Override
    public PDone expand(PCollection<KV<String, String>> input) {
      input.apply(ParDo.of(new MemcacheWriterFn(this)));
      return PDone.in(input.getPipeline());

    }

    abstract String hostname();
    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setHostname(String hostname);
//      have to add few more
      abstract Write build();
    }

    static class MemcacheWriterFn extends DoFn<KV<String, String>, Void> {

      private final Write spec;
//      there is also a withUserNameAndPassword
      private MemcacheClient client = MemcacheClientBuilder.newStringClient().connectAscii();

      MemcacheWriterFn(final Write spec) {this.spec = spec;}

      @StartBundle
      public void startBundle() {}

      @ProcessElement
      public void processElement(ProcessContext processContext) {
//        write to the client
      }

      @FinishBundle
      public void finishBundle() {}

      @Teardown
      public void tearDown() {}
    }
  }

}
