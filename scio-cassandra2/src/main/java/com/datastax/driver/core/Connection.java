/*
 *      Copyright (C) 2012-2015 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.core;

import com.datastax.driver.core.Responses.Result.SetKeyspace;
import com.datastax.driver.core.exceptions.AuthenticationException;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.exceptions.DriverInternalError;
import com.datastax.driver.core.utils.MoreFutures;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.MapMaker;
import com.google.common.util.concurrent.*;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.netty.util.TimerTask;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.net.ssl.SSLEngine;
import java.lang.ref.WeakReference;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.datastax.driver.core.Message.Response.Type.ERROR;
import static io.netty.handler.timeout.IdleState.ALL_IDLE;

// For LoggingHandler
//import org.jboss.netty.handler.logging.LoggingHandler;
//import org.jboss.netty.logging.InternalLogLevel;

/**
 * A connection to a Cassandra Node.
 */
class Connection {

    private static final Logger logger = LoggerFactory.getLogger(Connection.class);
    private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

    private static final boolean DISABLE_COALESCING = SystemProperties.getBoolean("com.datastax.driver.DISABLE_COALESCING", false);

    enum State {OPEN, TRASHED, RESURRECTING, GONE}

    final AtomicReference<State> state = new AtomicReference<State>(State.OPEN);

    volatile long maxIdleTime;

    public final InetSocketAddress address;
    private final String name;

    @VisibleForTesting
    volatile Channel channel;
    private final Factory factory;

    @VisibleForTesting
    final Dispatcher dispatcher;

    // Used by connection pooling to count how many requests are "in flight" on that connection.
    public final AtomicInteger inFlight = new AtomicInteger(0);

    private final AtomicInteger writer = new AtomicInteger(0);
    private volatile String keyspace;

    private volatile boolean isInitialized;
    private final AtomicBoolean isDefunct = new AtomicBoolean();
    private final AtomicBoolean signaled = new AtomicBoolean();

    private final AtomicReference<ConnectionCloseFuture> closeFuture = new AtomicReference<ConnectionCloseFuture>();

    private final AtomicReference<Owner> ownerRef = new AtomicReference<Owner>();

    /**
     * /**
     * Create a new connection to a Cassandra node and associate it with the given pool.
     *
     * @param name    the connection name
     * @param address the remote address
     * @param factory the connection factory to use
     * @param owner   the component owning this connection (may be null).
     *                Note that an existing connection can also be associated to an owner later with {@link #setOwner(Owner)}.
     */
    protected Connection(String name, InetSocketAddress address, Factory factory, Owner owner) {
        this.address = address;
        this.factory = factory;
        this.dispatcher = new Dispatcher();
        this.name = name;
        this.ownerRef.set(owner);
    }

    /**
     * Create a new connection to a Cassandra node.
     */
    Connection(String name, InetSocketAddress address, Factory factory) {
        this(name, address, factory, null);
    }

    public ListenableFuture<Void> initAsync() {
        if (factory.isShutdown)
            return Futures.immediateFailedFuture(new ConnectionException(address, "Connection factory is shut down"));

        ProtocolVersion protocolVersion = factory.protocolVersion == null ? ProtocolVersion.NEWEST_SUPPORTED : factory.protocolVersion;
        final SettableFuture<Void> channelReadyFuture = SettableFuture.create();

        try {
            Bootstrap bootstrap = factory.newBootstrap();
            ProtocolOptions protocolOptions = factory.configuration.getProtocolOptions();
            bootstrap.handler(
                    new Initializer(this, protocolVersion, protocolOptions.getCompression().compressor(), protocolOptions.getSSLOptions(),
                            factory.configuration.getPoolingOptions().getHeartbeatIntervalSeconds(),
                            factory.configuration.getNettyOptions()));

            ChannelFuture future = bootstrap.connect(address);

            writer.incrementAndGet();
            future.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    writer.decrementAndGet();
                    channel = future.channel();
                    if (isClosed()) {
                        channel.close().addListener(new ChannelFutureListener() {
                            @Override
                            public void operationComplete(ChannelFuture future) throws Exception {
                                channelReadyFuture.setException(new TransportException(Connection.this.address, "Connection closed during initialization."));
                            }
                        });
                    } else {
                        Connection.this.factory.allChannels.add(channel);
                        if (!future.isSuccess()) {
                            if (logger.isDebugEnabled())
                                logger.debug(String.format("%s Error connecting to %s%s", Connection.this, Connection.this.address, extractMessage(future.cause())));
                            channelReadyFuture.setException(new TransportException(Connection.this.address, "Cannot connect", future.cause()));
                        } else {
                            logger.debug("{} Connection established, initializing transport", Connection.this);
                            channel.closeFuture().addListener(new ChannelCloseListener());
                            channelReadyFuture.set(null);
                        }
                    }
                }
            });
        } catch (RuntimeException e) {
            closeAsync().force();
            throw e;
        }

        Executor initExecutor = factory.manager.configuration.getPoolingOptions().getInitializationExecutor();

        ListenableFuture<Void> initializeTransportFuture = Futures.transformAsync(channelReadyFuture,
                onChannelReady(protocolVersion, initExecutor), initExecutor);


        // Fallback on initializeTransportFuture so we can properly propagate specific exceptions.
        ListenableFuture<Void> initFuture = Futures.catchingAsync(initializeTransportFuture, Throwable.class, new AsyncFunction<Throwable, Void>() {
            @Override
            public ListenableFuture<Void> apply(@Nullable Throwable t) throws Exception {
                SettableFuture<Void> future = SettableFuture.create();
                // Make sure the connection gets properly closed.
                if (t instanceof ClusterNameMismatchException || t instanceof UnsupportedProtocolVersionException) {
                    // Just propagate
                    closeAsync().force();
                    future.setException(t);
                } else {
                    // Defunct to ensure that the error will be signaled (marking the host down)
                    Exception e = (t instanceof ConnectionException || t instanceof DriverException || t instanceof InterruptedException)
                            ? (Exception) t
                            : new ConnectionException(Connection.this.address,
                            String.format("Unexpected error during transport initialization (%s)", t),
                            t);
                    future.setException(defunct(e));
                }
                return future;
            }
        }, initExecutor);

        // Ensure the connection gets closed if the caller cancels the returned future.
        Futures.addCallback(initFuture, new MoreFutures.FailureCallback<Void>() {
            @Override
            public void onFailure(Throwable t) {
                if (!isClosed()) {
                    closeAsync().force();
                }
            }
        }, initExecutor);

        return initFuture;
    }

    private static String extractMessage(Throwable t) {
        if (t == null)
            return "";
        String msg = t.getMessage() == null || t.getMessage().isEmpty()
                ? t.toString()
                : t.getMessage();
        return " (" + msg + ')';
    }

    private AsyncFunction<Void, Void> onChannelReady(final ProtocolVersion protocolVersion, final Executor initExecutor) {
        return new AsyncFunction<Void, Void>() {
            @Override
            public ListenableFuture<Void> apply(Void input) throws Exception {
                ProtocolOptions.Compression compression = factory.configuration.getProtocolOptions().getCompression();
                Future startupResponseFuture = write(new Requests.Startup(compression));
                return Futures.transformAsync(startupResponseFuture,
                        onStartupResponse(protocolVersion, initExecutor), initExecutor);
            }
        };
    }

    private AsyncFunction<Message.Response, Void> onStartupResponse(final ProtocolVersion protocolVersion, final Executor initExecutor) {
        return new AsyncFunction<Message.Response, Void>() {
            @Override
            public ListenableFuture<Void> apply(Message.Response response) throws Exception {
                switch (response.type) {
                    case READY:
                        return checkClusterName(protocolVersion, initExecutor);
                    case ERROR:
                        Responses.Error error = (Responses.Error) response;
                        // Testing for a specific string is a tad fragile but well, we don't have much choice
                        if (error.code == ExceptionCode.PROTOCOL_ERROR && error.message.contains("Invalid or unsupported protocol version"))
                            throw unsupportedProtocolVersionException(protocolVersion, error.serverProtocolVersion);
                        throw new TransportException(address, String.format("Error initializing connection: %s", error.message));
                    case AUTHENTICATE:
                        Authenticator authenticator = factory.authProvider.newAuthenticator(address);
                        switch (protocolVersion) {
                            case V1:
                                if (authenticator instanceof ProtocolV1Authenticator)
                                    return authenticateV1(authenticator, protocolVersion, initExecutor);
                                else
                                    // DSE 3.x always uses SASL authentication backported from protocol v2
                                    return authenticateV2(authenticator, protocolVersion, initExecutor);
                            case V2:
                            case V3:
                                return authenticateV2(authenticator, protocolVersion, initExecutor);
                            default:
                                throw defunct(protocolVersion.unsupported());
                        }
                    default:
                        throw new TransportException(address, String.format("Unexpected %s response message from server to a STARTUP message", response.type));
                }
            }
        };
    }

    // Due to C* gossip bugs, system.peers may report nodes that are gone from the cluster.
    // If these nodes have been recommissionned to another cluster and are up, nothing prevents the driver from connecting
    // to them. So we check that the cluster the node thinks it belongs to is our cluster (JAVA-397).
    private ListenableFuture<Void> checkClusterName(ProtocolVersion protocolVersion, final Executor executor) {
        final String expected = factory.manager.metadata.clusterName;

        // At initialization, the cluster is not known yet
        if (expected == null) {
            markInitialized();
            return MoreFutures.VOID_SUCCESS;
        }

        DefaultResultSetFuture clusterNameFuture = new DefaultResultSetFuture(null, protocolVersion, new Requests.Query("select cluster_name from system.local"));
        try {
            write(clusterNameFuture);
            return Futures.transformAsync(clusterNameFuture,
                    new AsyncFunction<ResultSet, Void>() {
                        @Override
                        public ListenableFuture<Void> apply(ResultSet rs) throws Exception {
                            Row row = rs.one();
                            String actual = row.getString("cluster_name");
                            if (!expected.equals(actual))
                                throw new ClusterNameMismatchException(address, actual, expected);
                            markInitialized();
                            return MoreFutures.VOID_SUCCESS;
                        }
                    }, executor);
        } catch (Exception e) {
            return Futures.immediateFailedFuture(e);
        }
    }

    private void markInitialized() {
        isInitialized = true;
        Host.statesLogger.debug("[{}] {} Transport initialized, connection ready", address, this);
    }

    private ListenableFuture<Void> authenticateV1(Authenticator authenticator, final ProtocolVersion protocolVersion, final Executor executor) {
        Requests.Credentials creds = new Requests.Credentials(((ProtocolV1Authenticator) authenticator).getCredentials());
        try {
            Future authResponseFuture = write(creds);
            return Futures.transformAsync(authResponseFuture,
                    new AsyncFunction<Message.Response, Void>() {
                        @Override
                        public ListenableFuture<Void> apply(Message.Response authResponse) throws Exception {
                            switch (authResponse.type) {
                                case READY:
                                    return checkClusterName(protocolVersion, executor);
                                case ERROR:
                                    throw new AuthenticationException(address, ((Responses.Error) authResponse).message);
                                default:
                                    throw new TransportException(address, String.format("Unexpected %s response message from server to a CREDENTIALS message", authResponse.type));
                            }
                        }
                    }, executor);
        } catch (Exception e) {
            return Futures.immediateFailedFuture(e);
        }
    }

    private ListenableFuture<Void> authenticateV2(final Authenticator authenticator, final ProtocolVersion protocolVersion, final Executor executor) {
        byte[] initialResponse = authenticator.initialResponse();
        if (null == initialResponse)
            initialResponse = EMPTY_BYTE_ARRAY;

        try {
            Future authResponseFuture = write(new Requests.AuthResponse(initialResponse));
            return Futures.transformAsync(authResponseFuture, onV2AuthResponse(authenticator, protocolVersion, executor), executor);
        } catch (Exception e) {
            return Futures.immediateFailedFuture(e);
        }
    }

    private AsyncFunction<Message.Response, Void> onV2AuthResponse(final Authenticator authenticator, final ProtocolVersion protocolVersion, final Executor executor) {
        return new AsyncFunction<Message.Response, Void>() {
            @Override
            public ListenableFuture<Void> apply(Message.Response authResponse) throws Exception {
                switch (authResponse.type) {
                    case AUTH_SUCCESS:
                        logger.trace("{} Authentication complete", this);
                        authenticator.onAuthenticationSuccess(((Responses.AuthSuccess) authResponse).token);
                        return checkClusterName(protocolVersion, executor);
                    case AUTH_CHALLENGE:
                        byte[] responseToServer = authenticator.evaluateChallenge(((Responses.AuthChallenge) authResponse).token);
                        if (responseToServer == null) {
                            // If we generate a null response, then authentication has completed, proceed without
                            // sending a further response back to the server.
                            logger.trace("{} Authentication complete (No response to server)", this);
                            return checkClusterName(protocolVersion, executor);
                        } else {
                            // Otherwise, send the challenge response back to the server
                            logger.trace("{} Sending Auth response to challenge", this);
                            Future nextResponseFuture = write(new Requests.AuthResponse(responseToServer));
                            return Futures.transformAsync(nextResponseFuture, onV2AuthResponse(authenticator, protocolVersion, executor), executor);
                        }
                    case ERROR:
                        // This is not very nice, but we're trying to identify if we
                        // attempted v2 auth against a server which only supports v1
                        // The AIOOBE indicates that the server didn't recognise the
                        // initial AuthResponse message
                        String message = ((Responses.Error) authResponse).message;
                        if (message.startsWith("java.lang.ArrayIndexOutOfBoundsException: 15"))
                            message = String.format("Cannot use authenticator %s with protocol version 1, "
                                    + "only plain text authentication is supported with this protocol version", authenticator);
                        throw new AuthenticationException(address, message);
                    default:
                        throw new TransportException(address, String.format("Unexpected %s response message from server to authentication message", authResponse.type));
                }
            }
        };
    }

    private UnsupportedProtocolVersionException unsupportedProtocolVersionException(ProtocolVersion triedVersion, ProtocolVersion serverProtocolVersion) {
        logger.debug("Got unsupported protocol version error from {} for version {} server supports version {}", address, triedVersion, serverProtocolVersion);
        return new UnsupportedProtocolVersionException(address, triedVersion, serverProtocolVersion);
    }

    public boolean isDefunct() {
        return isDefunct.get();
    }

    public int maxAvailableStreams() {
        return dispatcher.streamIdHandler.maxAvailableStreams();
    }

    <E extends Exception> E defunct(E e) {
        if (isDefunct.compareAndSet(false, true)) {

            if (Host.statesLogger.isTraceEnabled())
                Host.statesLogger.trace("Defuncting " + this, e);
            else if (Host.statesLogger.isDebugEnabled())
                Host.statesLogger.debug("Defuncting {} because: {}", this, e.getMessage());


            Host host = factory.manager.metadata.getHost(address);
            if (host != null) {
                // Sometimes close() can be called before defunct(); avoid decrementing the connection count twice, but
                // we still want to signal the error to the conviction policy.
                boolean decrement = signaled.compareAndSet(false, true);

                boolean hostDown = host.convictionPolicy.signalConnectionFailure(this, decrement);
                if (hostDown) {
                    factory.manager.signalHostDown(host, host.wasJustAdded());
                } else {
                    notifyOwnerWhenDefunct();
                }
            }

            // Force the connection to close to make sure the future completes. Otherwise force() might never get called and
            // threads will wait on the future forever.
            // (this also errors out pending handlers)
            closeAsync().force();
        }
        return e;
    }

    private void notifyOwnerWhenDefunct() {
        // If an error happens during initialization, the owner will detect it and take appropriate action
        if (!isInitialized)
            return;

        Owner owner = this.ownerRef.get();
        if (owner != null)
            owner.onConnectionDefunct(this);
    }

    public String keyspace() {
        return keyspace;
    }

    public void setKeyspace(String keyspace) throws ConnectionException {
        if (keyspace == null)
            return;

        if (this.keyspace != null && this.keyspace.equals(keyspace))
            return;

        try {
            Uninterruptibles.getUninterruptibly(setKeyspaceAsync(keyspace));
        } catch (ConnectionException e) {
            throw defunct(e);
        } catch (BusyConnectionException e) {
            logger.warn("Tried to set the keyspace on busy {}. "
                    + "This should not happen but is not critical (it will be retried)", this);
            throw new ConnectionException(address, "Tried to set the keyspace on busy connection");
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof OperationTimedOutException) {
                // Rethrow so that the caller doesn't try to use the connection, but do not defunct as we don't want to mark down
                logger.warn("Timeout while setting keyspace on {}. "
                        + "This should not happen but is not critical (it will be retried)", this);
                throw new ConnectionException(address, "Timeout while setting keyspace on connection");
            } else {
                throw defunct(new ConnectionException(address, "Error while setting keyspace", cause));
            }
        }
    }

    ListenableFuture<Void> setKeyspaceAsync(final String keyspace) throws ConnectionException, BusyConnectionException {
        logger.trace("{} Setting keyspace {}", this, keyspace);
        // Note: we quote the keyspace below, because the name is the one coming from Cassandra, so it's in the right case already
        Future future = write(new Requests.Query("USE \"" + keyspace + '"'));
        return Futures.transformAsync(future, new AsyncFunction<Message.Response, Void>() {
            @Override
            public ListenableFuture<Void> apply(Message.Response response) throws Exception {
                if (response instanceof SetKeyspace) {
                    Connection.this.keyspace = ((SetKeyspace) response).keyspace;
                    return MoreFutures.VOID_SUCCESS;
                } else if (response.type == ERROR) {
                    Responses.Error error = (Responses.Error) response;
                    throw defunct(error.asException(address));
                } else {
                    throw defunct(new DriverInternalError("Unexpected response while setting keyspace: " + response));
                }
            }
        }, factory.manager.configuration.getPoolingOptions().getInitializationExecutor());
    }

    /**
     * Write a request on this connection.
     *
     * @param request the request to send
     * @return a future on the server response
     * @throws ConnectionException if the connection is closed
     * @throws TransportException  if an I/O error while sending the request
     */
    public Future write(Message.Request request) throws ConnectionException, BusyConnectionException {
        Future future = new Future(request);
        write(future);
        return future;
    }

    public ResponseHandler write(ResponseCallback callback) throws ConnectionException, BusyConnectionException {
        return write(callback, true);
    }

    public ResponseHandler write(ResponseCallback callback, boolean startTimeout) throws ConnectionException, BusyConnectionException {

        ResponseHandler handler = new ResponseHandler(this, callback);
        dispatcher.add(handler);

        Message.Request request = callback.request().setStreamId(handler.streamId);

        /*
         * We check for close/defunct *after* having set the handler because closing/defuncting
         * will set their flag and then error out handler if need. So, by doing the check after
         * having set the handler, we guarantee that even if we race with defunct/close, we may
         * never leave a handler that won't get an answer or be errored out.
         */
        if (isDefunct.get()) {
            dispatcher.removeHandler(handler, true);
            throw new ConnectionException(address, "Write attempt on defunct connection");
        }

        if (isClosed()) {
            dispatcher.removeHandler(handler, true);
            throw new ConnectionException(address, "Connection has been closed");
        }

        logger.trace("{}, stream {}, writing request {}", this, request.getStreamId(), request);
        writer.incrementAndGet();

        if (DISABLE_COALESCING) {
            channel.writeAndFlush(request).addListener(writeHandler(request, handler));
        } else {
            flush(new FlushItem(channel, request, writeHandler(request, handler)));
        }
        if (startTimeout)
            handler.startTimeout();

        return handler;
    }

    private ChannelFutureListener writeHandler(final Message.Request request, final ResponseHandler handler) {
        return new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture writeFuture) {

                writer.decrementAndGet();

                if (!writeFuture.isSuccess()) {
                    logger.debug("{}, stream {}, Error writing request {}", Connection.this, request.getStreamId(), request);
                    // Remove this handler from the dispatcher so it don't get notified of the error
                    // twice (we will fail that method already)
                    dispatcher.removeHandler(handler, true);

                    final ConnectionException ce;
                    if (writeFuture.cause() instanceof java.nio.channels.ClosedChannelException) {
                        ce = new TransportException(address, "Error writing: Closed channel");
                    } else {
                        ce = new TransportException(address, "Error writing", writeFuture.cause());
                    }
                    final long latency = System.nanoTime() - handler.startTime;
                    // This handler is executed while holding the writeLock of the channel.
                    // defunct might close the pool, which will close all of its connections; closing a connection also
                    // requires its writeLock.
                    // Therefore if multiple connections in the same pool get a write error, they could deadlock;
                    // we run defunct on a separate thread to avoid that.
                    ListeningExecutorService executor = factory.manager.executor;
                    if (!executor.isShutdown())
                        executor.execute(new Runnable() {
                            @Override
                            public void run() {
                                handler.callback.onException(Connection.this, defunct(ce), latency, handler.retryCount);
                            }
                        });
                } else {
                    logger.trace("{}, stream {}, request sent successfully", Connection.this, request.getStreamId());
                }
            }
        };
    }

    boolean hasOwner() {
        return this.ownerRef.get() != null;
    }

    /**
     * @return whether the connection was already associated with an owner
     */
    boolean setOwner(Owner owner) {
        return ownerRef.compareAndSet(null, owner);
    }

    /**
     * If the connection is part of a pool, return it to the pool.
     * The connection should generally not be reused after that.
     */
    void release() {
        Owner owner = ownerRef.get();
        if (owner instanceof HostConnectionPool)
            ((HostConnectionPool) owner).returnConnection(this);
    }

    public boolean isClosed() {
        return closeFuture.get() != null;
    }

    /**
     * Closes the connection: no new writes will be accepted after this method has returned.
     * <p/>
     * However, a closed connection might still have ongoing queries awaiting for their result.
     * When all these ongoing queries have completed, the underlying channel will be closed; we
     * refer to this final state as "terminated".
     *
     * @return a future that will complete once the connection has terminated.
     * @see #tryTerminate(boolean)
     */
    public CloseFuture closeAsync() {

        ConnectionCloseFuture future = new ConnectionCloseFuture();
        if (!closeFuture.compareAndSet(null, future)) {
            // close had already been called, return the existing future
            return closeFuture.get();
        }

        logger.debug("{} closing connection", this);

        // Only signal if defunct hasn't done it already
        if (signaled.compareAndSet(false, true)) {
            Host host = factory.manager.metadata.getHost(address);
            if (host != null) {
                host.convictionPolicy.signalConnectionClosed(this);
            }
        }

        boolean terminated = tryTerminate(false);
        if (!terminated) {
            // The time by which all pending requests should have normally completed (use twice the read timeout for a generous
            // estimate -- note that this does not cover the eventuality that read timeout is updated dynamically, but we can live
            // with that).
            long terminateTime = System.currentTimeMillis() + 2 * factory.getReadTimeoutMillis();
            factory.reaper.register(this, terminateTime);
        }
        return future;
    }

    /**
     * Tries to terminate a closed connection, i.e. release system resources.
     * <p/>
     * This is called both by "normal" code and by {@link Cluster.ConnectionReaper}.
     *
     * @param force whether to proceed if there are still outstanding requests.
     * @return whether the connection has actually terminated.
     * @see #closeAsync()
     */
    boolean tryTerminate(boolean force) {
        assert isClosed();
        ConnectionCloseFuture future = closeFuture.get();

        if (future.isDone()) {
            logger.debug("{} has already terminated", this);
            return true;
        } else {
            if (force || dispatcher.pending.isEmpty()) {
                if (force)
                    logger.warn("Forcing termination of {}. This should not happen and is likely a bug, please report.", this);
                future.force();
                return true;
            } else {
                logger.debug("Not terminating {}: there are still pending requests", this);
                return false;
            }
        }
    }

    @Override
    public String toString() {
        return String.format("Connection[%s, inFlight=%d, closed=%b]", name, inFlight.get(), isClosed());
    }

    public static class Factory {

        public final Timer timer;

        final EventLoopGroup eventLoopGroup;
        private final Class<? extends Channel> channelClass;

        private final ChannelGroup allChannels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);

        private final ConcurrentMap<Host, AtomicInteger> idGenerators = new ConcurrentHashMap<Host, AtomicInteger>();
        public final DefaultResponseHandler defaultHandler;
        final Cluster.Manager manager;
        final Cluster.ConnectionReaper reaper;
        public final Configuration configuration;

        public final AuthProvider authProvider;
        private volatile boolean isShutdown;

        volatile ProtocolVersion protocolVersion;
        private final NettyOptions nettyOptions;

        Factory(Cluster.Manager manager, Configuration configuration) {
            this.defaultHandler = manager;
            this.manager = manager;
            this.reaper = manager.reaper;
            this.configuration = configuration;
            this.authProvider = configuration.getProtocolOptions().getAuthProvider();
            this.protocolVersion = configuration.getProtocolOptions().initialProtocolVersion;
            this.nettyOptions = configuration.getNettyOptions();
            this.eventLoopGroup = nettyOptions.eventLoopGroup(manager.threadFactory("nio-worker"));
            this.channelClass = nettyOptions.channelClass();
            this.timer = nettyOptions.timer(manager.threadFactory("timeouter"));
        }

        public int getPort() {
            return configuration.getProtocolOptions().getPort();
        }

        /**
         * Opens a new connection to the node this factory points to.
         *
         * @return the newly created (and initialized) connection.
         * @throws ConnectionException if connection attempt fails.
         */
        public Connection open(Host host) throws ConnectionException, InterruptedException, UnsupportedProtocolVersionException, ClusterNameMismatchException {
            InetSocketAddress address = host.getSocketAddress();

            if (isShutdown)
                throw new ConnectionException(address, "Connection factory is shut down");

            host.convictionPolicy.signalConnectionsOpening(1);
            Connection connection = new Connection(buildConnectionName(host), address, this);
            // This method opens the connection synchronously, so wait until it's initialized
            try {
                connection.initAsync().get();
                return connection;
            } catch (ExecutionException e) {
                throw launderAsyncInitException(e);
            }
        }

        /**
         * Same as open, but associate the created connection to the provided connection pool.
         */
        public Connection open(HostConnectionPool pool) throws ConnectionException, InterruptedException, UnsupportedProtocolVersionException, ClusterNameMismatchException {

            pool.host.convictionPolicy.signalConnectionsOpening(1);
            Connection connection = new Connection(buildConnectionName(pool.host), pool.host.getSocketAddress(), this, pool);
            try {
                connection.initAsync().get();
                return connection;
            } catch (ExecutionException e) {
                throw launderAsyncInitException(e);
            }
        }

        /**
         * Creates new connections and associate them to the provided connection pool, but does not start them.
         */
        public List<Connection> newConnections(HostConnectionPool pool, int count) {
            pool.host.convictionPolicy.signalConnectionsOpening(count);
            List<Connection> connections = Lists.newArrayListWithCapacity(count);
            for (int i = 0; i < count; i++)
                connections.add(new Connection(buildConnectionName(pool.host), pool.host.getSocketAddress(), this, pool));
            return connections;
        }

        private String buildConnectionName(Host host) {
            return host.getSocketAddress().toString() + '-' + getIdGenerator(host).getAndIncrement();
        }

        static RuntimeException launderAsyncInitException(ExecutionException e) throws ConnectionException, InterruptedException, UnsupportedProtocolVersionException, ClusterNameMismatchException {
            Throwable t = e.getCause();
            if (t instanceof ConnectionException)
                throw (ConnectionException) t;
            if (t instanceof InterruptedException)
                throw (InterruptedException) t;
            if (t instanceof UnsupportedProtocolVersionException)
                throw (UnsupportedProtocolVersionException) t;
            if (t instanceof ClusterNameMismatchException)
                throw (ClusterNameMismatchException) t;
            if (t instanceof DriverException)
                throw (DriverException) t;

            return new RuntimeException("Unexpected exception during connection initialization", t);
        }

        private AtomicInteger getIdGenerator(Host host) {
            AtomicInteger g = idGenerators.get(host);
            if (g == null) {
                g = new AtomicInteger(1);
                AtomicInteger old = idGenerators.putIfAbsent(host, g);
                if (old != null)
                    g = old;
            }
            return g;
        }

        public long getReadTimeoutMillis() {
            return configuration.getSocketOptions().getReadTimeoutMillis();
        }

        private Bootstrap newBootstrap() {
            Bootstrap b = new Bootstrap();
            b.group(eventLoopGroup)
                    .channel(channelClass);

            SocketOptions options = configuration.getSocketOptions();

            b.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, options.getConnectTimeoutMillis());
            Boolean keepAlive = options.getKeepAlive();
            if (keepAlive != null)
                b.option(ChannelOption.SO_KEEPALIVE, keepAlive);
            Boolean reuseAddress = options.getReuseAddress();
            if (reuseAddress != null)
                b.option(ChannelOption.SO_REUSEADDR, reuseAddress);
            Integer soLinger = options.getSoLinger();
            if (soLinger != null)
                b.option(ChannelOption.SO_LINGER, soLinger);
            Boolean tcpNoDelay = options.getTcpNoDelay();
            if (tcpNoDelay != null)
                b.option(ChannelOption.TCP_NODELAY, tcpNoDelay);
            Integer receiveBufferSize = options.getReceiveBufferSize();
            if (receiveBufferSize != null)
                b.option(ChannelOption.SO_RCVBUF, receiveBufferSize);
            Integer sendBufferSize = options.getSendBufferSize();
            if (sendBufferSize != null)
                b.option(ChannelOption.SO_SNDBUF, sendBufferSize);

            nettyOptions.afterBootstrapInitialized(b);
            return b;
        }

        public void shutdown() {
            // Make sure we skip creating connection from now on.
            isShutdown = true;

            // All channels should be closed already, we call this just to be sure. And we know
            // we're not on an I/O thread or anything, so just call await.
            allChannels.close().awaitUninterruptibly();

            nettyOptions.onClusterClose(eventLoopGroup);
            nettyOptions.onClusterClose(timer);
        }
    }

    private static final class Flusher implements Runnable {
        final WeakReference<EventLoop> eventLoopRef;
        final Queue<FlushItem> queued = new ConcurrentLinkedQueue<FlushItem>();
        final AtomicBoolean running = new AtomicBoolean(false);
        final HashSet<Channel> channels = new HashSet<Channel>();
        int runsWithNoWork = 0;

        private Flusher(EventLoop eventLoop) {
            this.eventLoopRef = new WeakReference<EventLoop>(eventLoop);
        }

        void start() {
            if (!running.get() && running.compareAndSet(false, true)) {
                EventLoop eventLoop = eventLoopRef.get();
                if (eventLoop != null)
                    eventLoop.execute(this);
            }
        }

        @Override
        public void run() {

            boolean doneWork = false;
            FlushItem flush;
            while (null != (flush = queued.poll())) {
                Channel channel = flush.channel;
                if (channel.isActive()) {
                    channels.add(channel);
                    channel.write(flush.request).addListener(flush.listener);
                    doneWork = true;
                }
            }

            // Always flush what we have (don't artificially delay to try to coalesce more messages)
            for (Channel channel : channels)
                channel.flush();
            channels.clear();

            if (doneWork) {
                runsWithNoWork = 0;
            } else {
                // either reschedule or cancel
                if (++runsWithNoWork > 5) {
                    running.set(false);
                    if (queued.isEmpty() || !running.compareAndSet(false, true))
                        return;
                }
            }

            EventLoop eventLoop = eventLoopRef.get();
            if (eventLoop != null && !eventLoop.isShuttingDown()) {
                eventLoop.schedule(this, 10000, TimeUnit.NANOSECONDS);
            }
        }
    }

    private static final ConcurrentMap<EventLoop, Flusher> flusherLookup = new MapMaker()
            .concurrencyLevel(16)
            .weakKeys()
            .makeMap();

    private static class FlushItem {
        final Channel channel;
        final Object request;
        final ChannelFutureListener listener;

        private FlushItem(Channel channel, Object request, ChannelFutureListener listener) {
            this.channel = channel;
            this.request = request;
            this.listener = listener;
        }
    }

    private void flush(FlushItem item) {
        EventLoop loop = item.channel.eventLoop();
        Flusher flusher = flusherLookup.get(loop);
        if (flusher == null) {
            Flusher alt = flusherLookup.putIfAbsent(loop, flusher = new Flusher(loop));
            if (alt != null)
                flusher = alt;
        }

        flusher.queued.add(item);
        flusher.start();
    }

    class Dispatcher extends SimpleChannelInboundHandler<Message.Response> {

        public final StreamIdGenerator streamIdHandler;
        private final ConcurrentMap<Integer, ResponseHandler> pending = new ConcurrentHashMap<Integer, ResponseHandler>();

        Dispatcher() {
            ProtocolVersion protocolVersion = factory.protocolVersion;
            if (protocolVersion == null) {
                // This happens for the first control connection because the protocol version has not been
                // negociated yet.
                protocolVersion = ProtocolVersion.V2;
            }
            streamIdHandler = StreamIdGenerator.newInstance(protocolVersion);
        }

        public void add(ResponseHandler handler) {
            ResponseHandler old = pending.put(handler.streamId, handler);
            assert old == null;
        }

        public void removeHandler(ResponseHandler handler, boolean releaseStreamId) {

            // If we don't release the ID, mark first so that we can rely later on the fact that if
            // we receive a response for an ID with no handler, it's that this ID has been marked.
            if (!releaseStreamId)
                streamIdHandler.mark(handler.streamId);

            // If a RequestHandler is cancelled right when the response arrives, this method (called with releaseStreamId=false) will race with messageReceived.
            // messageReceived could have already released the streamId, which could have already been reused by another request. We must not remove the handler
            // if it's not ours, because that would cause the other request to hang forever.
            boolean removed = pending.remove(handler.streamId, handler);
            if (!removed) {
                // We raced, so if we marked the streamId above, that was wrong.
                if (!releaseStreamId)
                    streamIdHandler.unmark(handler.streamId);
                return;
            }
            handler.cancelTimeout();

            if (releaseStreamId)
                streamIdHandler.release(handler.streamId);

            if (isClosed())
                tryTerminate(false);
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Message.Response response) throws Exception {
            int streamId = response.getStreamId();

            if (logger.isTraceEnabled())
                logger.trace("{}, stream {}, received: {}", Connection.this, streamId, asDebugString(response));

            if (streamId < 0) {
                factory.defaultHandler.handle(response);
                return;
            }

            ResponseHandler handler = pending.remove(streamId);
            streamIdHandler.release(streamId);
            if (handler == null) {
                /**
                 * During normal operation, we should not receive responses for which we don't have a handler. There is
                 * two cases however where this can happen:
                 *   1) The connection has been defuncted due to some internal error and we've raced between removing the
                 *      handler and actually closing the connection; since the original error has been logged, we're fine
                 *      ignoring this completely.
                 *   2) This request has timed out. In that case, we've already switched to another host (or errored out
                 *      to the user). So log it for debugging purpose, but it's fine ignoring otherwise.
                 */
                streamIdHandler.unmark(streamId);
                if (logger.isDebugEnabled())
                    logger.debug("{} Response received on stream {} but no handler set anymore (either the request has "
                            + "timed out or it was closed due to another error). Received message is {}", Connection.this, streamId, asDebugString(response));
                return;
            }
            handler.cancelTimeout();
            handler.callback.onSet(Connection.this, response, System.nanoTime() - handler.startTime, handler.retryCount);

            // If we happen to be closed and we're the last outstanding request, we need to terminate the connection
            // (note: this is racy as the signaling can be called more than once, but that's not a problem)
            if (isClosed())
                tryTerminate(false);
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if (!isClosed() && evt instanceof IdleStateEvent && ((IdleStateEvent) evt).state() == ALL_IDLE) {
                logger.debug("{} was inactive for {} seconds, sending heartbeat", Connection.this, factory.configuration.getPoolingOptions().getHeartbeatIntervalSeconds());
                write(HEARTBEAT_CALLBACK);
            }
        }

        // Make sure we don't print huge responses in debug/error logs.
        private String asDebugString(Object obj) {
            if (obj == null)
                return "null";

            String msg = obj.toString();
            if (msg.length() < 500)
                return msg;

            return msg.substring(0, 500) + "... [message of size " + msg.length() + " truncated]";
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            if (logger.isDebugEnabled())
                logger.debug(String.format("%s connection error", Connection.this), cause);

            // Ignore exception while writing, this will be handled by write() directly
            if (writer.get() > 0)
                return;

            defunct(new TransportException(address, String.format("Unexpected exception triggered (%s)", cause), cause));
        }

        public void errorOutAllHandler(ConnectionException ce) {
            Iterator<ResponseHandler> iter = pending.values().iterator();
            while (iter.hasNext()) {
                ResponseHandler handler = iter.next();
                handler.cancelTimeout();
                handler.callback.onException(Connection.this, ce, System.nanoTime() - handler.startTime, handler.retryCount);
                iter.remove();
            }
        }
    }

    private class ChannelCloseListener implements ChannelFutureListener {
        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
            // If we've closed the channel client side then we don't really want to defunct the connection, but
            // if there is remaining thread waiting on us, we still want to wake them up
            if (!isInitialized || isClosed()) {
                dispatcher.errorOutAllHandler(new TransportException(address, "Channel has been closed"));
                // we still want to force so that the future completes
                Connection.this.closeAsync().force();
            } else
                defunct(new TransportException(address, "Channel has been closed"));
        }
    }

    private static final ResponseCallback HEARTBEAT_CALLBACK = new ResponseCallback() {

        @Override
        public Message.Request request() {
            return new Requests.Options();
        }

        @Override
        public int retryCount() {
            return 0; // no retries here
        }

        @Override
        public void onSet(Connection connection, Message.Response response, long latency, int retryCount) {
            switch (response.type) {
                case SUPPORTED:
                    logger.debug("{} heartbeat query succeeded", connection);
                    break;
                default:
                    fail(connection, new ConnectionException(connection.address, "Unexpected heartbeat response: " + response));
            }
        }

        @Override
        public void onException(Connection connection, Exception exception, long latency, int retryCount) {
            // Nothing to do: the connection is already defunct if we arrive here
        }

        @Override
        public boolean onTimeout(Connection connection, long latency, int retryCount) {
            fail(connection, new ConnectionException(connection.address, "Heartbeat query timed out"));
            return true;
        }

        private void fail(Connection connection, Exception e) {
            connection.defunct(e);
        }
    };

    private class ConnectionCloseFuture extends CloseFuture {

        @Override
        public ConnectionCloseFuture force() {
            // Note: we must not call releaseExternalResources on the bootstrap, because this shutdown the executors, which are shared

            // This method can be thrown during initialization, at which point channel is not yet set. This is ok.
            if (channel == null) {
                set(null);
                return this;
            }

            // We're going to close this channel. If anyone is waiting on that connection, we should defunct it otherwise it'll wait
            // forever. In general this won't happen since we get there only when all ongoing query are done, but this can happen
            // if the shutdown is forced. This is a no-op if there is no handler set anymore.
            dispatcher.errorOutAllHandler(new TransportException(address, "Connection has been closed"));

            ChannelFuture future = channel.close();
            future.addListener(new ChannelFutureListener() {
                public void operationComplete(ChannelFuture future) {
                    factory.allChannels.remove(channel);
                    if (future.cause() != null) {
                        logger.warn("Error closing channel", future.cause());
                        ConnectionCloseFuture.this.setException(future.cause());
                    } else
                        ConnectionCloseFuture.this.set(null);
                }
            });
            return this;
        }
    }

    static class Future extends AbstractFuture<Message.Response> implements RequestHandler.Callback {

        private final Message.Request request;
        private volatile InetSocketAddress address;

        public Future(Message.Request request) {
            this.request = request;
        }

        @Override
        public void register(RequestHandler handler) {
            // noop, we don't care about the handler here so far
        }

        @Override
        public Message.Request request() {
            return request;
        }

        @Override
        public int retryCount() {
            // This is ignored, as there is no retry logic in this class
            return 0;
        }

        @Override
        public void onSet(Connection connection, Message.Response response, ExecutionInfo info, Statement statement, long latency) {
            onSet(connection, response, latency, 0);
        }

        @Override
        public void onSet(Connection connection, Message.Response response, long latency, int retryCount) {
            this.address = connection.address;
            super.set(response);
        }

        @Override
        public void onException(Connection connection, Exception exception, long latency, int retryCount) {
            // If all nodes are down, we will get a null connection here. This is fine, if we have
            // an exception, consumers shouldn't assume the address is not null.
            if (connection != null)
                this.address = connection.address;
            super.setException(exception);
        }

        @Override
        public boolean onTimeout(Connection connection, long latency, int retryCount) {
            assert connection != null; // We always timeout on a specific connection, so this shouldn't be null
            this.address = connection.address;
            return super.setException(new OperationTimedOutException(connection.address));
        }

        public InetSocketAddress getAddress() {
            return address;
        }
    }

    interface ResponseCallback {
        public Message.Request request();

        public int retryCount();

        public void onSet(Connection connection, Message.Response response, long latency, int retryCount);

        public void onException(Connection connection, Exception exception, long latency, int retryCount);

        public boolean onTimeout(Connection connection, long latency, int retryCount);
    }

    static class ResponseHandler {

        public final Connection connection;
        public final int streamId;
        public final ResponseCallback callback;
        public final int retryCount;

        private final long startTime;
        private volatile Timeout timeout;

        private final AtomicBoolean isCancelled = new AtomicBoolean();

        public ResponseHandler(Connection connection, ResponseCallback callback) throws BusyConnectionException {
            this.connection = connection;
            this.streamId = connection.dispatcher.streamIdHandler.next();
            this.callback = callback;
            this.retryCount = callback.retryCount();

            this.startTime = System.nanoTime();
        }

        void startTimeout() {
            long timeoutMs = connection.factory.getReadTimeoutMillis();
            this.timeout = timeoutMs <= 0 ? null : connection.factory.timer.newTimeout(onTimeoutTask(), timeoutMs, TimeUnit.MILLISECONDS);
        }

        void cancelTimeout() {
            if (timeout != null)
                timeout.cancel();
        }

        public boolean cancelHandler() {
            if (!isCancelled.compareAndSet(false, true))
                return false;

            // We haven't really received a response: we want to remove the handle because we gave up on that
            // request and there is no point in holding the handler, but we don't release the streamId. If we
            // were, a new request could reuse that ID but get the answer to the request we just gave up on instead
            // of its own answer, and we would have no way to detect that.
            connection.dispatcher.removeHandler(this, false);
            return true;
        }

        private TimerTask onTimeoutTask() {
            return new TimerTask() {
                @Override
                public void run(Timeout timeout) {
                    if (callback.onTimeout(connection, System.nanoTime() - startTime, retryCount))
                        cancelHandler();
                }
            };
        }
    }

    public interface DefaultResponseHandler {
        public void handle(Message.Response response);
    }

    private static class Initializer extends ChannelInitializer<SocketChannel> {
        // Stateless handlers
        private static final Message.ProtocolDecoder messageDecoder = new Message.ProtocolDecoder();
        private static final Message.ProtocolEncoder messageEncoderV1 = new Message.ProtocolEncoder(ProtocolVersion.V1);
        private static final Message.ProtocolEncoder messageEncoderV2 = new Message.ProtocolEncoder(ProtocolVersion.V2);
        private static final Message.ProtocolEncoder messageEncoderV3 = new Message.ProtocolEncoder(ProtocolVersion.V3);
        private static final Frame.Encoder frameEncoder = new Frame.Encoder();

        private final ProtocolVersion protocolVersion;
        private final Connection connection;
        private final FrameCompressor compressor;
        private final SSLOptions sslOptions;
        private final NettyOptions nettyOptions;
        private final ChannelHandler idleStateHandler;

        public Initializer(Connection connection, ProtocolVersion protocolVersion, FrameCompressor compressor, SSLOptions sslOptions, int heartBeatIntervalSeconds, NettyOptions nettyOptions) {
            this.connection = connection;
            this.protocolVersion = protocolVersion;
            this.compressor = compressor;
            this.sslOptions = sslOptions;
            this.nettyOptions = nettyOptions;
            this.idleStateHandler = new IdleStateHandler(0, 0, heartBeatIntervalSeconds);
        }

        @Override
        protected void initChannel(SocketChannel channel) throws Exception {
            ChannelPipeline pipeline = channel.pipeline();

            if (sslOptions != null) {
                SSLEngine engine = sslOptions.context.createSSLEngine();
                engine.setUseClientMode(true);
                engine.setEnabledCipherSuites(sslOptions.cipherSuites);
                SslHandler handler = new SslHandler(engine);
                pipeline.addLast("ssl", handler);
            }

//            pipeline.addLast("debug", new LoggingHandler(LogLevel.INFO));

            pipeline.addLast("frameDecoder", new Frame.Decoder());
            pipeline.addLast("frameEncoder", frameEncoder);

            if (compressor != null) {
                pipeline.addLast("frameDecompressor", new Frame.Decompressor(compressor));
                pipeline.addLast("frameCompressor", new Frame.Compressor(compressor));
            }

            pipeline.addLast("messageDecoder", messageDecoder);
            pipeline.addLast("messageEncoder", messageEncoderFor(protocolVersion));

            pipeline.addLast("idleStateHandler", idleStateHandler);

            pipeline.addLast("dispatcher", connection.dispatcher);

            nettyOptions.afterChannelInitialized(channel);
        }

        private Message.ProtocolEncoder messageEncoderFor(ProtocolVersion version) {
            switch (version) {
                case V1:
                    return messageEncoderV1;
                case V2:
                    return messageEncoderV2;
                case V3:
                    return messageEncoderV3;
                default:
                    throw new DriverInternalError("Unsupported protocol version " + protocolVersion);
            }
        }
    }

    /**
     * A component that "owns" a connection, and should be notified when it dies.
     */
    interface Owner {
        void onConnectionDefunct(Connection connection);
    }
}
