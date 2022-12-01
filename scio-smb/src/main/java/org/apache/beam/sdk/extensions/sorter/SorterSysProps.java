package org.apache.beam.sdk.extensions.sorter;

public class SorterSysProps {
  public static String getTempLocation() {
    return System.getProperty("smb.sorter.tmpdir", "/tmp");
  }
}
