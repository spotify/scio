package com.spotify.scio.memcache;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import com.spotify.folsom.MemcacheClient;
import com.spotify.folsom.MemcacheClientBuilder;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;

@AutoValue
public abstract class MemcacheConnectionConfiguration implements Serializable {

    abstract String host();

    abstract int port();

    abstract int ttl();

    abstract int flushDelay();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
        abstract Builder setHost(String host);

        abstract Builder setPort(int port);

        abstract Builder setTtl(int ttl);

        abstract Builder setFlushDelay(int flushDelay);

        abstract MemcacheConnectionConfiguration build();
    }
//
//    public static MemcacheConnectionConfiguration create() {
//        return new AutoValue_MemcacheConnectionConfiguration.Builder()
//                .setHost(ValueProvider.StaticValueProvider.of(MemcacheClient))
//                .setPort(port)
//                .setTtl(ttl)
//                .setFlushDelay(flushDelay)
//                .build();
//    }

    public static MemcacheConnectionConfiguration create(String host, int port, int ttl, int flushDelay) {
        return new AutoValue_MemcacheConnectionConfiguration.Builder()
                .setHost(host)
                .setPort(port)
                .setTtl(ttl)
                .setFlushDelay(flushDelay)
                .build();
    }

    /**
     * Define the host of the Memcache server.
     */
    public MemcacheConnectionConfiguration withHost(String host) {
        checkArgument(host != null, "host can not be null");
        return builder().setHost(host).build();
    }

    /**
     * TTL will be use for all the records writen to memcache server
     */
    public MemcacheConnectionConfiguration withTtl(int ttl) {
        return builder().setTtl(ttl).build();
    }

    /**
     * TTL will be use for all the records writen to memcache server
     */
    public MemcacheConnectionConfiguration flushDelay(int delay) {
        return builder().setFlushDelay(delay).build();
    }

    /**
     * Define the port number of the Memcache server.
     */
    public MemcacheConnectionConfiguration withPort(int port) {
        checkArgument(port > 0, "port can not be negative or 0");
        return builder().setPort(port).build();
    }

    public MemcacheClient<String> connect() {
        //        TODO: add with username and password
//        TODO add .withChannelClass()
        return MemcacheClientBuilder.newStringClient()
                .withAddress(host())
                .withKeyCharset(StandardCharsets.UTF_8)

                .connectAscii();
    }
}