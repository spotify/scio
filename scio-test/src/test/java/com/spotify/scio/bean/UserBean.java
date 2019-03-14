/*
 * Copyright 2019 Spotify AB.
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
package com.spotify.scio.bean;

final public class UserBean {

  private String name;
  private Integer age;

  public UserBean(){}

  public UserBean(String name, Integer age) {
    this.name = name;
    this.age = age;
  }

  public String getName() {
    return this.name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public Integer getAge() {
    return this.age;
  }

  public void setAge(Integer age) {
    this.age = age;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null)
      return false;

    if (!(obj instanceof UserBean))
      return false;

    if (obj == this)
      return true;

    return
      this.getName().equals(((UserBean) obj).getName()) &&
      this.getAge().equals(((UserBean) obj).getAge());
  }

  @Override
  public String toString() {
    return String.format("UserBean(%s, %d)", this.getName(), this.getAge());
  }
}