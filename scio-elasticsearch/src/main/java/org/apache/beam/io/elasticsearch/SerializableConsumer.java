package org.apache.beam.io.elasticsearch;

import java.io.Serializable;
import java.util.function.Consumer;

public interface SerializableConsumer<T> extends Consumer<T>, Serializable {

}
