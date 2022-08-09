package org.apache.beam.sdk.io;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.nio.channels.WritableByteChannel;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.util.MimeTypes;

public class TFRecordFileBasedSink extends FileBasedSink<byte[], Void, byte[]> {
  public TFRecordFileBasedSink(
      ValueProvider<ResourceId> tempDirectoryProvider,
      FileBasedSink.DynamicDestinations<byte[], Void, byte[]> dynamicDestinations,
      Compression compression) {
    super(tempDirectoryProvider, dynamicDestinations, compression);
  }

  @Override
  public WriteOperation<Void, byte[]> createWriteOperation() {
    return new WriteOperation<Void, byte[]>(this) {

      @Override
      public Writer<Void, byte[]> createWriter() throws Exception {
        return new Writer<Void, byte[]>(this, MimeTypes.BINARY) {
          private WritableByteChannel outChannel;
          /* package private :( */
          private /* TFRecordIO.TFRecordCodec */ Object codec;
          private Method writeMethod;

          @Override
          protected void prepareWrite(final WritableByteChannel channel) throws Exception {
            this.outChannel = channel;
            // FIXME why! make! things! private?
            Class<?> inner = Class.forName("org.apache.beam.sdk.io.TFRecordIO$TFRecordCodec");
            Constructor<?> ctor = inner.getDeclaredConstructor();
            ctor.setAccessible(true);
            this.codec = ctor.newInstance();
            this.writeMethod = inner.getMethod("write", WritableByteChannel.class, byte[].class);
            this.writeMethod.setAccessible(true);
          }

          @Override
          public void write(final byte[] value) throws Exception {
            /* codec.write(outChannel, value); */
            writeMethod.invoke(this.codec, outChannel, value);
          }
        };
      }
    };
  }
}
