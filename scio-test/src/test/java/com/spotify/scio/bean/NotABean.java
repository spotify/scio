package com.spotify.scio.bean;

final public class NotABean {

  private String name;
  private Integer age;

  public NotABean(String name, Integer age) {
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
}