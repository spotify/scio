/*
Copyright 2013 Twitter, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package com.spotify.scio.vendor.chill;

import com.esotericsoftware.kryo.Kryo;
import java.io.Serializable;
import org.objenesis.strategy.InstantiatorStrategy;

/**
 * Class to create a new Kryo instance. Used in initial configuration or pooling of Kryo objects.
 * These objects are immutable (and hopefully Kryo serializable)
 */
public class KryoInstantiator implements Serializable {
  public Kryo newKryo() {
    return new Kryo();
  }

  /** Use this to set a specific classloader */
  public KryoInstantiator setClassLoader(final ClassLoader cl) {
    return new KryoInstantiator() {
      public Kryo newKryo() {
        Kryo k = KryoInstantiator.this.newKryo();
        k.setClassLoader(cl);
        return k;
      }
    };
  }
  /** If true, Kryo will error if it sees a class that has not been registered */
  public KryoInstantiator setInstantiatorStrategy(final InstantiatorStrategy inst) {
    return new KryoInstantiator() {
      public Kryo newKryo() {
        Kryo k = KryoInstantiator.this.newKryo();
        k.setInstantiatorStrategy(inst);
        return k;
      }
    };
  }

  /**
   * If true, Kryo keeps a map of all the objects it has seen. this can use a ton of memory on
   * hadoop, but save serialization costs in some cases
   */
  public KryoInstantiator setReferences(final boolean ref) {
    return new KryoInstantiator() {
      public Kryo newKryo() {
        Kryo k = KryoInstantiator.this.newKryo();
        /**
         * Kryo 2.17, used in storm, has this method returning void, 2.21 has it returning boolean.
         * Try not to call the method if you don't need to.
         */
        if (k.getReferences() != ref) {
          k.setReferences(ref);
        }
        return k;
      }
    };
  }

  /** If true, Kryo will error if it sees a class that has not been registered */
  public KryoInstantiator setRegistrationRequired(final boolean req) {
    return new KryoInstantiator() {
      public Kryo newKryo() {
        Kryo k = KryoInstantiator.this.newKryo();
        /**
         * Try to avoid calling this method if you don't need to. We've been burned by binary
         * compatibility with Kryo
         */
        if (k.isRegistrationRequired() != req) {
          k.setRegistrationRequired(req);
        }
        return k;
      }
    };
  }
  /**
   * Use Thread.currentThread().getContextClassLoader() as the ClassLoader where ther newKryo is
   * called
   */
  public KryoInstantiator setThreadContextClassLoader() {
    return new KryoInstantiator() {
      public Kryo newKryo() {
        Kryo k = KryoInstantiator.this.newKryo();
        k.setClassLoader(Thread.currentThread().getContextClassLoader());
        return k;
      }
    };
  }

  public KryoInstantiator withRegistrar(final IKryoRegistrar r) {
    return new KryoInstantiator() {
      public Kryo newKryo() {
        Kryo k = KryoInstantiator.this.newKryo();
        r.apply(k);
        return k;
      }
    };
  }
}
