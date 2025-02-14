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

/** Particular systems subclass this to reuse existing configured Instantiators */
public abstract class Config {
  /** Return null if this key is undefined */
  public abstract String get(String key);

  public abstract void set(String key, String value);

  public String getOrElse(String key, String def) {
    String val = get(key);
    if (null == val) {
      return def;
    } else {
      return val;
    }
  }

  public boolean contains(String key) {
    return get(key) != null;
  }

  public Boolean getBoolean(String key) {
    String bval = get(key);
    if (null == bval) {
      return null;
    } else {
      return Boolean.valueOf(bval);
    }
  }

  public boolean getBoolean(String key, boolean defval) {
    String bval = get(key);
    if (null == bval) {
      return defval;
    } else {
      return Boolean.valueOf(bval).booleanValue();
    }
  }

  public void setBoolean(String key, Boolean v) {
    if (null == v) {
      set(key, null);
    } else {
      set(key, v.toString());
    }
  }
}
