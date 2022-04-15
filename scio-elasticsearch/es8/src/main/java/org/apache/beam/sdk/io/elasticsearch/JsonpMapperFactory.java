package org.apache.beam.sdk.io.elasticsearch;

import co.elastic.clients.json.JsonpMapper;
import java.io.Serializable;

public interface JsonpMapperFactory extends Serializable {

  JsonpMapper create();
}
