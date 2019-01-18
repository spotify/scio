package com.spotify.scio.bean;

final public class TypeMismatch {

  private String name;
  private Integer age;

  public TypeMismatch(String name, Integer age) {
    this.name = name;
    this.age = age;
  }

  public String getName() {
    return this.name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getAge() {
    return this.age.toString();
  }

  public void setAge(Integer age) {
    this.age = age;
  }
}