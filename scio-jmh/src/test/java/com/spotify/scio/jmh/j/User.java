package com.spotify.scio.jmh.j;

final public class User {

  private UserId id;
  private String name;
  private String email;

  public User(UserId id, String name, String email) {
    this.id = id;
    this.name = name;
    this.email = email;
  }

  public UserId getId() {
    return this.id;
  }

  public void setId(UserId id) {
    this.id = id;
  }

  public String getName() {
    return this.name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getEmail() {
    return this.email;
  }

  public void setEmail(String email) {
    this.email = email;
  }
}