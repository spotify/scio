/*
 * Copyright 2016 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.google.cloud.dataflow.contrib.hadoop;

import com.google.common.base.Preconditions;

/**
 * Utils for Hadoop/HDFS Authentication. It's externalizable so that we can pass it around
 * as part of Source/Bundles.
 */
public class HadoopUserUtils {
//  private static String user;
//
//  private static final String DEFAULT_USERNAME = "default_username";
  private static final String HADOOP_USER_NAME_KEY = "HADOOP_USER_NAME";

//  static {
//    String localUser = System.getProperty("user.name");
//    if (localUser == null || localUser.isEmpty()) {
//      user = DEFAULT_USERNAME;
//    } else {
//      user = localUser;
//    }
//  }

  public static String getSimpleAuthUser() {
    return "";
  }

  public static void setSimpleAuthUser(String user) {
    Preconditions.checkNotNull(user);
    Preconditions.checkArgument(!user.isEmpty());

    // Keep in mind that this has to happen before Filesystem object is created.
    System.setProperty(HADOOP_USER_NAME_KEY, user);
//    HadoopUserUtils.user = user;
  }

//  @Override
//  public void writeExternal(ObjectOutput out) throws IOException {
//    out.writeUTF(user);
//  }
//
//  @Override
//  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
//    HadoopUserUtils.user = in.readUTF();
//    // set property on deserialization to keep jvm up to date
//    System.setProperty(HADOOP_USER_NAME_KEY, user);
//  }
}
