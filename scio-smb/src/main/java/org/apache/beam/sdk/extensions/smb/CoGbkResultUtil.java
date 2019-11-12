// FIXME: BEAM expose private constructor
package org.apache.beam.sdk.extensions.smb;

import com.spotify.scio.smb.annotations.PatchedFromBeam;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGbkResultSchema;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;

@PatchedFromBeam(origin="org.apache.beam.sdk.transforms.join.CoGbkResult")
public class CoGbkResultUtil {
  public static CoGbkResult newCoGbkResult(CoGbkResultSchema schema, List<Iterable<?>> valueMap) {
    try {
      Constructor<CoGbkResult> ctor = CoGbkResult.class
          .getDeclaredConstructor(CoGbkResultSchema.class, List.class);
      ctor.setAccessible(true);
      return ctor.newInstance(schema, valueMap);
    } catch (NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }
}