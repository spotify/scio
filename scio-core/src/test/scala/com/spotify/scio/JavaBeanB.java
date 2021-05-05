package com.spotify.scio;

class JavaBeanB implements java.io.Serializable {
   private String name = null;
   private String uuid = null;
   private int money = 0;

   public JavaBeanB() {
   }
   public String getName(){
      return name;
   }
   public String getUuid(){
      return uuid;
   }
   public int getMoney(){
      return money;
   }
   public void setName(String name){
      this.name = name;
   }
   public void setUuid(String uuid){
      this.uuid = uuid;
   }
   public void setMoney(int money){
      this.money = money;
   }
}