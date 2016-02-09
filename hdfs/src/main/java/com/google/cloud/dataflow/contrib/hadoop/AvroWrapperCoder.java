package com.google.cloud.dataflow.contrib.hadoop;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.CoderException;
import com.google.cloud.dataflow.sdk.coders.StandardCoder;
import com.google.cloud.dataflow.sdk.util.CloudObject;
import com.google.cloud.dataflow.sdk.util.PropertyNames;
import com.google.common.base.Preconditions;
import org.apache.avro.mapred.AvroWrapper;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;

/**
 * A {@code AvroWrapperCoder} is a {@link com.google.cloud.dataflow.sdk.coders.Coder} for a Java
 * class that implements {@link org.apache.avro.mapred.AvroWrapper}.
 *
 * @param <W> the type of the wrapper
 * @param <D> the type of the datum
 */
public class AvroWrapperCoder<W extends AvroWrapper<D>, D> extends StandardCoder<W> {
  private static final long serialVersionUID = 0L;

  private final Class<W> wrapperType;
  private final AvroCoder<D> datumCoder;

  private AvroWrapperCoder(Class<W> wrapperType, AvroCoder<D> datumCoder) {
    this.wrapperType = wrapperType;
    this.datumCoder = datumCoder;
  }

  /**
   * Return a {@code AvroWrapperCoder} instance for the provided element class.
   * @param <W> the type of the wrapper
   * @param <D> the type of the datum
   */
  public static <W extends AvroWrapper<D>, D>
  AvroWrapperCoder<W, D>of(Class<W> wrapperType, AvroCoder<D> datumCoder) {
    return new AvroWrapperCoder<>(wrapperType, datumCoder);
  }

  @JsonCreator
  @SuppressWarnings("unchecked")
  public static AvroWrapperCoder<?, ?> of(
      @JsonProperty("wrapperType") String wrapperType,
      @JsonProperty(PropertyNames.COMPONENT_ENCODINGS) List<Coder<?>> components)
      throws ClassNotFoundException {
    Class<?> clazz = Class.forName(wrapperType);
    if (!AvroWrapper.class.isAssignableFrom(clazz)) {
      throw new ClassNotFoundException(
          "Class " + wrapperType + " does not implement AvroWrapper");
    }
    Preconditions.checkArgument(components.size() == 1,
        "Expecting 1 component, got " + components.size());
    return of((Class<? extends AvroWrapper>) clazz, (AvroCoder<?>) components.get(0));
  }

  @Override
  public void encode(W value, OutputStream outStream, Context context) throws IOException {
    datumCoder.encode(value.datum(), outStream, context);
  }

  @Override
  public W decode(InputStream inStream, Context context) throws IOException {
    try {
      W wrapper = wrapperType.newInstance();
      wrapper.datum(datumCoder.decode(inStream, context));
      return wrapper;
    } catch (InstantiationException | IllegalAccessException e) {
      throw new CoderException("unable to deserialize record", e);
    }
  }

  @Override
  public List<? extends Coder<?>> getCoderArguments() {
    return Collections.singletonList(datumCoder);
  }

  @Override
  public CloudObject asCloudObject() {
    CloudObject result = super.asCloudObject();
    result.put("wrapperType", wrapperType.getName());
    return result;
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {}

}
