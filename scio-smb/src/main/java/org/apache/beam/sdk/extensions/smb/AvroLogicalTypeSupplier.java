package org.apache.beam.sdk.extensions.smb;

import org.apache.avro.Conversions;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericData;
import org.apache.avro.specific.SpecificData;
import org.apache.parquet.avro.SpecificDataSupplier;

public class AvroLogicalTypeSupplier extends SpecificDataSupplier {
  @Override
  public GenericData get() {
    return new SpecificData() {
      {
        addLogicalTypeConversion(new TimeConversions.DateConversion());
        addLogicalTypeConversion(new TimeConversions.TimeConversion());
        addLogicalTypeConversion(new TimeConversions.TimestampConversion());
        addLogicalTypeConversion(new Conversions.DecimalConversion());
      }
    };
  }
}
