package org.apache.beam.sdk.io;

import com.spotify.scio.smb.annotations.PatchedFromBeam;
import org.apache.beam.sdk.io.fs.MatchResult;

@PatchedFromBeam(origin="org.apache.beam.sdk.io.FileIO")
public class ReadableFileUtil {
  public static FileIO.ReadableFile newReadableFile(MatchResult.Metadata metadata, Compression compression) {
    return new FileIO.ReadableFile(metadata, compression);
  }
}