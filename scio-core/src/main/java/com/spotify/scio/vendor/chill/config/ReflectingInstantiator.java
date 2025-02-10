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

package com.spotify.scio.vendor.chill.config;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.spotify.scio.vendor.chill.ClassRegistrar;
import com.spotify.scio.vendor.chill.IKryoRegistrar;
import com.spotify.scio.vendor.chill.KryoInstantiator;
import com.spotify.scio.vendor.chill.ReflectingDefaultRegistrar;
import com.spotify.scio.vendor.chill.ReflectingRegistrar;
import java.util.ArrayList;
import java.util.List;
import org.objenesis.strategy.InstantiatorStrategy;
import org.objenesis.strategy.StdInstantiatorStrategy;

public class ReflectingInstantiator extends KryoInstantiator {

  final boolean regRequired;
  final boolean skipMissing;
  final Class<? extends Kryo> kryoClass;
  final Class<? extends InstantiatorStrategy> instStratClass;
  final List<IKryoRegistrar> registrations;
  final List<ReflectingDefaultRegistrar> defaultRegistrations;

  public ReflectingInstantiator(Config conf) throws ConfigurationException {
    regRequired = conf.getBoolean(REGISTRATION_REQUIRED, REGISTRATION_REQUIRED_DEFAULT);
    skipMissing = conf.getBoolean(SKIP_MISSING, SKIP_MISSING_DEFAULT);

    try {
      kryoClass =
          (Class<? extends Kryo>)
              Class.forName(
                  conf.getOrElse(KRYO_CLASS, KRYO_CLASS_DEFAULT),
                  true,
                  Thread.currentThread().getContextClassLoader());
      instStratClass =
          (Class<? extends InstantiatorStrategy>)
              Class.forName(
                  conf.getOrElse(INSTANTIATOR_STRATEGY_CLASS, INSTANTIATOR_STRATEGY_CLASS_DEFAULT),
                  true,
                  Thread.currentThread().getContextClassLoader());

      registrations = (List<IKryoRegistrar>) buildRegistrars(conf.get(REGISTRATIONS), false);
      defaultRegistrations =
          (List<ReflectingDefaultRegistrar>) buildRegistrars(conf.get(DEFAULT_REGISTRATIONS), true);
      // Make sure we can make a newKryo, this throws a runtime exception if not.
      newKryoWithEx();
    } catch (ClassNotFoundException x) {
      throw new ConfigurationException(x);
    } catch (InstantiationException x) {
      throw new ConfigurationException(x);
    } catch (IllegalAccessException x) {
      throw new ConfigurationException(x);
    }
  }

  /** Create an instance using the defaults for non-listed params */
  public ReflectingInstantiator(
      Iterable<ClassRegistrar> classRegistrations,
      Iterable<ReflectingRegistrar> registrations,
      Iterable<ReflectingDefaultRegistrar> defaults) {
    this(
        Kryo.class,
        StdInstantiatorStrategy.class,
        classRegistrations,
        registrations,
        defaults,
        REGISTRATION_REQUIRED_DEFAULT,
        SKIP_MISSING_DEFAULT);
  }

  public ReflectingInstantiator(
      Class<? extends Kryo> kryoClass,
      Class<? extends InstantiatorStrategy> stratClass,
      Iterable<ClassRegistrar> classRegistrations,
      Iterable<ReflectingRegistrar> registrations,
      Iterable<ReflectingDefaultRegistrar> defaults,
      boolean regRequired,
      boolean skipMissing) {

    this.kryoClass = kryoClass;
    instStratClass = stratClass;
    this.regRequired = regRequired;
    this.skipMissing = skipMissing;

    this.registrations = new ArrayList<IKryoRegistrar>();
    for (IKryoRegistrar cr : classRegistrations) {
      this.registrations.add(cr);
    }
    for (IKryoRegistrar rr : registrations) {
      this.registrations.add(rr);
    }

    defaultRegistrations = new ArrayList<ReflectingDefaultRegistrar>();
    for (ReflectingDefaultRegistrar rdr : defaults) {
      defaultRegistrations.add(rdr);
    }
  }

  public void set(Config conf) throws ConfigurationException {
    conf.setBoolean(REGISTRATION_REQUIRED, regRequired);
    conf.setBoolean(SKIP_MISSING, skipMissing);

    conf.set(KRYO_CLASS, kryoClass.getName());
    conf.set(INSTANTIATOR_STRATEGY_CLASS, instStratClass.getName());

    conf.set(REGISTRATIONS, registrarsToString(registrations));
    conf.set(DEFAULT_REGISTRATIONS, registrarsToString(defaultRegistrations));
  }

  // This one adds expeption annotations that the interface does not have
  protected Kryo newKryoWithEx() throws InstantiationException, IllegalAccessException {
    Kryo k = kryoClass.newInstance();
    k.setInstantiatorStrategy(instStratClass.newInstance());
    k.setRegistrationRequired(regRequired);
    for (IKryoRegistrar kr : registrations) {
      kr.apply(k);
    }
    for (IKryoRegistrar dkr : defaultRegistrations) {
      dkr.apply(k);
    }
    return k;
  }

  @Override
  public Kryo newKryo() {
    try {
      return newKryoWithEx();
    } catch (InstantiationException x) {
      throw new RuntimeException(x);
    } catch (IllegalAccessException x) {
      throw new RuntimeException(x);
    }
  }

  /** All keys are prefixed with this string */
  public static final String prefix = "com.spotify.scio.vendor.chill.config.reflectinginstantiator";

  /**
   * Name of the subclass of kryo to instantiate to start with. If this is empty, we use Kryo.class
   */
  public static final String KRYO_CLASS = prefix + ".kryoclass";

  public static final String KRYO_CLASS_DEFAULT = Kryo.class.getName();
  /**
   * Name of the InstatiatorStrategy to use. If this is empty, we use
   * org.objenesis.strategy.StdInstantiatorStrategy
   */
  public static final String INSTANTIATOR_STRATEGY_CLASS = prefix + ".instantiatorstrategyclass";

  public static final String INSTANTIATOR_STRATEGY_CLASS_DEFAULT =
      StdInstantiatorStrategy.class.getName();
  /**
   * KRYO_REGISTRATIONS holds a colon-separated list of classes to register with Kryo. For example,
   * the following value:
   *
   * <p>"someClass,someSerializer:otherClass:thirdClass,thirdSerializer"
   *
   * <p>will direct KryoFactory to register someClass and thirdClass with custom serializers and
   * otherClass with Kryo's FieldsSerializer.
   */
  public static final String REGISTRATIONS = prefix + ".registrations";

  /**
   * DEFAULT_REGISTRATIONS holds a colon-separated list of classes or interfaces to register with
   * Kryo. Default Registrations are searched after basic registrations, and have the ability to
   * capture objects that are assignable from the hierarchy's superclass. For example, the following
   * value:
   *
   * <p>"someClass,someSerializer:someInterface,otherSerializer"
   *
   * <p>will configure to serializeobjects that extend from someClass with someSerializer, and
   * objects that extend someInterface with otherSerializer.
   */
  public static final String DEFAULT_REGISTRATIONS = prefix + ".defaultregistrations";

  /**
   * If SKIP_MISSING is set to false, Kryo will throw an error when Cascading tries to register a
   * class or serialization that doesn't exist.
   */
  public static final String SKIP_MISSING = prefix + ".skipmissing";

  public static final boolean SKIP_MISSING_DEFAULT = false;

  /**
   * If REGISTRATION_REQUIRED is set to false, Kryo will try to serialize all java objects, not just
   * those with custom serializations registered.
   */
  public static final String REGISTRATION_REQUIRED = prefix + ".registrationrequired";

  public static final boolean REGISTRATION_REQUIRED_DEFAULT = false;

  protected List<? extends IKryoRegistrar> buildRegistrars(String base, boolean isAddDefault)
      throws ConfigurationException {
    List<IKryoRegistrar> builder = new ArrayList<IKryoRegistrar>();

    if (base == null) return builder;

    for (String s : base.split(":")) {
      String[] pair = s.split(",");
      try {
        switch (pair.length) {
          case 1:
            if (isAddDefault) {
              throw new ConfigurationException(
                  "default serializers require class and serializer: " + base);
            }
            builder.add(
                new ClassRegistrar(
                    Class.forName(pair[0], true, Thread.currentThread().getContextClassLoader())));
            break;
          case 2:
            @SuppressWarnings("unchecked")
            Class kls =
                Class.forName(pair[0], true, Thread.currentThread().getContextClassLoader());
            Class<? extends Serializer> serializerClass =
                (Class<? extends Serializer>)
                    Class.forName(pair[1], true, Thread.currentThread().getContextClassLoader());
            if (isAddDefault) {
              builder.add(new ReflectingDefaultRegistrar(kls, serializerClass));
            } else {
              builder.add(new ReflectingRegistrar(kls, serializerClass));
            }
            break;
          default:
            throw new ConfigurationException(base + " is not well-formed.");
        }
      } catch (ClassNotFoundException e) {
        if (skipMissing) {
          System.err.println(
              "Could not find serialization or class for " + pair[1] + ". Skipping registration.");
        } else {
          throw new ConfigurationException(e);
        }
      }
    }
    return builder;
  }

  protected String registrarsToString(Iterable<? extends IKryoRegistrar> registrars)
      throws ConfigurationException {
    StringBuilder builder = new StringBuilder();
    boolean isFirst = true;
    for (IKryoRegistrar reg : registrars) {
      if (!isFirst) builder.append(":");
      isFirst = false;
      String part = null;
      if (reg instanceof ClassRegistrar) {
        ClassRegistrar r = (ClassRegistrar) reg;
        part = r.getRegisteredClass().getName();
      } else if (reg instanceof ReflectingRegistrar) {
        ReflectingRegistrar r = (ReflectingRegistrar) reg;
        part = r.getRegisteredClass().getName() + "," + r.getSerializerClass().getName();
      } else if (reg instanceof ReflectingDefaultRegistrar) {
        ReflectingDefaultRegistrar r = (ReflectingDefaultRegistrar) reg;
        part = r.getRegisteredClass().getName() + "," + r.getSerializerClass().getName();
      } else {
        throw new ConfigurationException(
            "Unknown type of reflecting registrar: " + reg.getClass().getName());
      }
      builder.append(part);
    }
    return builder.toString();
  }

  @Override
  public int hashCode() {
    return kryoClass.hashCode() ^ registrations.hashCode() ^ defaultRegistrations.hashCode();
  }

  @Override
  public boolean equals(Object that) {
    if (null == that) {
      return false;
    } else if (that instanceof ReflectingInstantiator) {
      ReflectingInstantiator thatri = (ReflectingInstantiator) that;
      return (regRequired == thatri.regRequired)
          && (skipMissing == thatri.skipMissing)
          && kryoClass.equals(thatri.kryoClass)
          && instStratClass.equals(thatri.instStratClass)
          && registrations.equals(thatri.registrations)
          && defaultRegistrations.equals(thatri.defaultRegistrations);
    } else {
      return false;
    }
  }
}
