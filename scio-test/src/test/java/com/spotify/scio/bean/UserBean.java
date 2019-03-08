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