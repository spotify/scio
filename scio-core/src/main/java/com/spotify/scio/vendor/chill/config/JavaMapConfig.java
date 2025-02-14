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

import java.util.Map;

/** This takes a raw Map and calls toString on the objects before returning them as values */
public class JavaMapConfig extends Config {

  final Map conf;

  public JavaMapConfig(Map conf) {
    this.conf = conf;
  }

  public JavaMapConfig() {
    this(new java.util.HashMap<String, String>());
  }
  /** Return null if this key is undefined */
  /** Return null if this key is undefined */
  @Override
  public String get(String key) {
    Object value = conf.get(key);
    if (null != value) {
      return value.toString();
    } else {
      return null;
    }
  }

  @Override
  public void set(String key, String value) {
    conf.put(key, value);
  }
}
