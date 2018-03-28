package com.spotify.scio.coders

import org.apache.commons.pool2.impl.{DefaultPooledObject, GenericObjectPool}
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.apache.commons.pool2.{ObjectPool, PooledObject, PooledObjectFactory}

object ObjectPoolUtils {


  def createPool[T](factory: () => T) : ObjectPool[T] = {
    new GenericObjectPool[T](new PooledObjectFactory[T] {

      override def destroyObject(p: PooledObject[T]): Unit = { /* noop */ }

      override def makeObject(): PooledObject[T] = new DefaultPooledObject(factory.apply())

      override def validateObject(p: PooledObject[T]): Boolean = true

      override def passivateObject(p: PooledObject[T]): Unit = {/* noop */}

      override def activateObject(p: PooledObject[T]): Unit = {/* noop */}

    }, config())
  }

  def withPool[T,R](pool: ObjectPool[T])(user: T => R) : R = {
    val value = pool.borrowObject()
    try {
      user.apply(value)
    } finally {
      pool.returnObject(value)
    }
  }

  def config() : GenericObjectPoolConfig = {
    val config = new GenericObjectPoolConfig()
    config.setMaxTotal(Integer.MAX_VALUE)
    config.setJmxEnabled(false)
    config.setMaxIdle(Integer.MAX_VALUE)
    config.setMinIdle(Integer.MAX_VALUE)
    config
  }

}
