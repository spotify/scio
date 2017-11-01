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

import com.datastax.driver.core.Message.Response;
import com.datastax.driver.core.exceptions.DriverInternalError;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.exceptions.UnsupportedFeatureException;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.ReconnectionPolicy;
import com.datastax.driver.core.policies.SpeculativeExecutionPolicy;
import com.datastax.driver.core.utils.MoreFutures;
import com.google.common.base.Functions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.*;
import io.netty.util.concurrent.EventExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Driver implementation of the Session interface.
 */
class SessionManager extends AbstractSession {

    private static final Logger logger = LoggerFactory.getLogger(Session.class);

    private static final boolean CHECK_IO_DEADLOCKS = SystemProperties.getBoolean(
            "com.datastax.driver.CHECK_IO_DEADLOCKS", true);

    final Cluster cluster;
    final ConcurrentMap<Host, HostConnectionPool> pools;
    final HostConnectionPool.PoolState poolsState;
    private final AtomicReference<ListenableFuture<Session>> initFuture = new AtomicReference<ListenableFuture<Session>>();
    final AtomicReference<CloseFuture> closeFuture = new AtomicReference<CloseFuture>();

    private volatile boolean isInit;
    private volatile boolean isClosing;

    // Package protected, only Cluster should construct that.
    SessionManager(Cluster cluster) {
        this.cluster = cluster;
        this.pools = new ConcurrentHashMap<Host, HostConnectionPool>();
        this.poolsState = new HostConnectionPool.PoolState();
    }

    @Override
    public Session init() {
        try {
            return Uninterruptibles.getUninterruptibly(initAsync());
        } catch (ExecutionException e) {
            throw DriverThrowables.propagateCause(e);
        }
    }

    @Override
    public ListenableFuture<Session> initAsync() {
        // If we haven't initialized the cluster, do it now
        cluster.init();

        ListenableFuture<Session> existing = initFuture.get();
        if (existing != null)
            return existing;

        final SettableFuture<Session> myInitFuture = SettableFuture.create();
        if (!initFuture.compareAndSet(null, myInitFuture))
            return initFuture.get();

        Collection<Host> hosts = cluster.getMetadata().allHosts();
        ListenableFuture<?> allPoolsCreatedFuture = createPools(hosts);
        ListenableFuture<?> allPoolsUpdatedFuture = Futures.transformAsync(allPoolsCreatedFuture,
                new AsyncFunction<Object, Object>() {
                    @Override
                    public ListenableFuture<Object> apply(Object input) throws Exception {
                        isInit = true;
                        return (ListenableFuture<Object>) updateCreatedPools();
                    }
                });

        Futures.addCallback(allPoolsUpdatedFuture, new FutureCallback<Object>() {
            @Override
            public void onSuccess(Object result) {
                myInitFuture.set(SessionManager.this);
            }

            @Override
            public void onFailure(Throwable t) {
                SessionManager.this.closeAsync(); // don't leak the session
                myInitFuture.setException(t);
            }
        });
        return myInitFuture;
    }

    private ListenableFuture<?> createPools(Collection<Host> hosts) {
        List<ListenableFuture<Boolean>> futures = Lists.newArrayListWithCapacity(hosts.size());
        for (Host host : hosts)
            if (host.state != Host.State.DOWN)
                futures.add(maybeAddPool(host, null));
        return Futures.allAsList(futures);
    }

    @Override
    public String getLoggedKeyspace() {
        return poolsState.keyspace;
    }

    @Override
    public ResultSetFuture executeAsync(final Statement statement) {
        if (isInit) {
            DefaultResultSetFuture future = new DefaultResultSetFuture(this, cluster.manager.protocolVersion(), makeRequestMessage(statement, null));
            new RequestHandler(this, future, statement).sendRequest();
            return future;
        } else {
            // If the session is not initialized, we can't call makeRequestMessage() synchronously, because it
            // requires internal Cluster state that might not be initialized yet (like the protocol version).
            // Because of the way the future is built, we need another 'proxy' future that we can return now.
            final ChainedResultSetFuture chainedFuture = new ChainedResultSetFuture();
            this.initAsync().addListener(new Runnable() {
                @Override
                public void run() {
                    DefaultResultSetFuture actualFuture = new DefaultResultSetFuture(SessionManager.this, cluster.manager.protocolVersion(), makeRequestMessage(statement, null));
                    execute(actualFuture, statement);
                    chainedFuture.setSource(actualFuture);
                }
            }, executor());
            return chainedFuture;
        }
    }

    @Override
    public ListenableFuture<PreparedStatement> prepareAsync(String query) {
        Connection.Future future = new Connection.Future(new Requests.Prepare(query));
        execute(future, Statement.DEFAULT);
        return toPreparedStatement(query, future);
    }

    @Override
    public CloseFuture closeAsync() {
        CloseFuture future = closeFuture.get();
        if (future != null)
            return future;

        isClosing = true;
        cluster.manager.removeSession(this);

        List<CloseFuture> futures = new ArrayList<CloseFuture>(pools.size());
        for (HostConnectionPool pool : pools.values())
            futures.add(pool.closeAsync());

        future = new CloseFuture.Forwarding(futures);

        return closeFuture.compareAndSet(null, future)
                ? future
                : closeFuture.get(); // We raced, it's ok, return the future that was actually set
    }

    @Override
    public boolean isClosed() {
        return closeFuture.get() != null;
    }

    @Override
    public Cluster getCluster() {
        return cluster;
    }

    @Override
    public Session.State getState() {
        return new State(this);
    }

    private ListenableFuture<PreparedStatement> toPreparedStatement(final String query, final Connection.Future future) {
        return Futures.transformAsync(future, new AsyncFunction<Response, PreparedStatement>() {
            @Override
            public ListenableFuture<PreparedStatement> apply(Response response) {
                switch (response.type) {
                    case RESULT:
                        Responses.Result rm = (Responses.Result) response;
                        switch (rm.kind) {
                            case PREPARED:
                                Responses.Result.Prepared pmsg = (Responses.Result.Prepared) rm;
                                PreparedStatement stmt = DefaultPreparedStatement.fromMessage(pmsg, cluster.getMetadata(), cluster.getConfiguration().getProtocolOptions().getProtocolVersionEnum(), query, poolsState.keyspace);
                                stmt = cluster.manager.addPrepared(stmt);
                                if (cluster.getConfiguration().getQueryOptions().isPrepareOnAllHosts()) {
                                    // All Sessions are connected to the same nodes so it's enough to prepare only the nodes of this session.
                                    // If that changes, we'll have to make sure this propagate to other sessions too.
                                    return prepare(stmt, future.getAddress());
                                } else {
                                    return Futures.immediateFuture(stmt);
                                }
                            default:
                                return Futures.immediateFailedFuture(
                                        new DriverInternalError(String.format("%s response received when prepared statement was expected", rm.kind)));
                        }
                    case ERROR:
                        return Futures.immediateFailedFuture(
                                ((Responses.Error) response).asException(future.getAddress()));
                    default:
                        return Futures.immediateFailedFuture(
                                new DriverInternalError(String.format("%s response received when prepared statement was expected", response.type)));
                }
            }
        }, executor());
    }

    Connection.Factory connectionFactory() {
        return cluster.manager.connectionFactory;
    }

    Configuration configuration() {
        return cluster.manager.configuration;
    }

    LoadBalancingPolicy loadBalancingPolicy() {
        return cluster.manager.loadBalancingPolicy();
    }

    SpeculativeExecutionPolicy speculativeRetryPolicy() {
        return cluster.manager.speculativeRetryPolicy();
    }

    ReconnectionPolicy reconnectionPolicy() {
        return cluster.manager.reconnectionPolicy();
    }

    ListeningExecutorService executor() {
        return cluster.manager.executor;
    }

    ListeningExecutorService blockingExecutor() {
        return cluster.manager.blockingExecutor;
    }

    // Returns whether there was problem creating the pool
    ListenableFuture<Boolean> forceRenewPool(final Host host, Connection reusedConnection) {
        final HostDistance distance = cluster.manager.loadBalancingPolicy().distance(host);
        if (distance == HostDistance.IGNORED)
            return Futures.immediateFuture(true);

        if (isClosing)
            return Futures.immediateFuture(false);

        final HostConnectionPool newPool = new HostConnectionPool(host, distance, this);
        ListenableFuture<Void> poolInitFuture = newPool.initAsync(reusedConnection);

        final SettableFuture<Boolean> future = SettableFuture.create();

        Futures.addCallback(poolInitFuture, new FutureCallback<Void>() {
            @Override
            public void onSuccess(Void result) {
                HostConnectionPool previous = pools.put(host, newPool);
                if (previous == null) {
                    logger.debug("Added connection pool for {}", host);
                } else {
                    logger.debug("Renewed connection pool for {}", host);
                    previous.closeAsync();
                }

                // If we raced with a session shutdown, ensure that the pool will be closed.
                if (isClosing) {
                    newPool.closeAsync();
                    pools.remove(host);
                    future.set(false);
                } else {
                    future.set(true);
                }
            }

            @Override
            public void onFailure(Throwable t) {
                logger.warn("Error creating pool to " + host, t);
                future.set(false);
            }
        });

        return future;
    }

    // Replace pool for a given host only if it's the given previous value (which can be null)
    // This returns a future if the replacement was successful, or null if we raced.
    private ListenableFuture<Void> replacePool(final Host host, HostDistance distance, HostConnectionPool previous, Connection reusedConnection) {
        if (isClosing)
            return MoreFutures.VOID_SUCCESS;

        final HostConnectionPool newPool = new HostConnectionPool(host, distance, this);
        if (previous == null) {
            if (pools.putIfAbsent(host, newPool) != null) {
                return null;
            }
        } else {
            if (!pools.replace(host, previous, newPool)) {
                return null;
            }
            if (!previous.isClosed()) {
                logger.warn("Replacing a pool that wasn't closed. Closing it now, but this was not expected.");
                previous.closeAsync();
            }
        }

        ListenableFuture<Void> poolInitFuture = newPool.initAsync(reusedConnection);

        Futures.addCallback(poolInitFuture, new FutureCallback<Void>() {
            @Override
            public void onSuccess(Void result) {
                // If we raced with a session shutdown, ensure that the pool will be closed.
                if (isClosing) {
                    newPool.closeAsync();
                    pools.remove(host);
                }
            }

            @Override
            public void onFailure(Throwable t) {
                pools.remove(host);
            }
        });
        return poolInitFuture;
    }

    // Returns whether there was problem creating the pool
    ListenableFuture<Boolean> maybeAddPool(final Host host, Connection reusedConnection) {
        final HostDistance distance = cluster.manager.loadBalancingPolicy().distance(host);
        if (distance == HostDistance.IGNORED)
            return Futures.immediateFuture(true);

        HostConnectionPool previous = pools.get(host);
        if (previous != null && !previous.isClosed())
            return Futures.immediateFuture(true);

        while (true) {
            previous = pools.get(host);
            if (previous != null && !previous.isClosed())
                return Futures.immediateFuture(true);

            final SettableFuture<Boolean> future = SettableFuture.create();
            ListenableFuture<Void> newPoolInit = replacePool(host, distance, previous, reusedConnection);
            if (newPoolInit != null) {
                Futures.addCallback(newPoolInit, new FutureCallback<Void>() {
                    @Override
                    public void onSuccess(Void result) {
                        logger.debug("Added connection pool for {}", host);
                        future.set(true);
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        if (t instanceof UnsupportedProtocolVersionException) {
                            cluster.manager.logUnsupportedVersionProtocol(host, ((UnsupportedProtocolVersionException) t).unsupportedVersion);
                            cluster.manager.triggerOnDown(host, false);
                        } else if (t instanceof ClusterNameMismatchException) {
                            ClusterNameMismatchException e = (ClusterNameMismatchException) t;
                            cluster.manager.logClusterNameMismatch(host, e.expectedClusterName, e.actualClusterName);
                            cluster.manager.triggerOnDown(host, false);
                        } else {
                            logger.warn("Error creating pool to " + host, t);
                        }
                        future.set(false);
                    }
                });
                return future;
            }
        }
    }

    CloseFuture removePool(Host host) {
        final HostConnectionPool pool = pools.remove(host);
        return pool == null
                ? CloseFuture.immediateFuture()
                : pool.closeAsync();
    }

    /*
     * When the set of live nodes change, the loadbalancer will change his
     * mind on host distances. It might change it on the node that came/left
     * but also on other nodes (for instance, if a node dies, another
     * previously ignored node may be now considered).
     *
     * This method ensures that all hosts for which a pool should exist
     * have one, and hosts that shouldn't don't.
     */
    ListenableFuture<?> updateCreatedPools() {
        // This method does nothing during initialization. Some hosts may be non-responsive but not yet marked DOWN; if
        // we execute the code below we would try to create their pool over and over again.
        // It's called explicitly at the end of init(), once isInit has been set to true.
        if (!isInit)
            return MoreFutures.VOID_SUCCESS;

        // We do 2 iterations, so that we add missing pools first, and them remove all unecessary pool second.
        // That way, we'll avoid situation where we'll temporarily lose connectivity
        final List<Host> toRemove = new ArrayList<Host>();
        List<ListenableFuture<Boolean>> poolCreatedFutures = Lists.newArrayList();

        for (Host h : cluster.getMetadata().allHosts()) {
            HostDistance dist = loadBalancingPolicy().distance(h);
            HostConnectionPool pool = pools.get(h);

            if (pool == null) {
                if (dist != HostDistance.IGNORED && h.state == Host.State.UP)
                    poolCreatedFutures.add(maybeAddPool(h, null));
            } else if (dist != pool.hostDistance) {
                if (dist == HostDistance.IGNORED) {
                    toRemove.add(h);
                } else {
                    pool.hostDistance = dist;
                    pool.ensureCoreConnections();
                }
            }
        }

        // Wait pool creation before removing, so we don't lose connectivity
        ListenableFuture<?> allPoolsCreatedFuture = Futures.successfulAsList(poolCreatedFutures);

        return Futures.transformAsync(allPoolsCreatedFuture, new AsyncFunction<Object, List<Void>>() {
            @Override
            public ListenableFuture<List<Void>> apply(Object input) throws Exception {
                List<ListenableFuture<Void>> poolRemovedFuture = Lists.newArrayListWithCapacity(toRemove.size());
                for (Host h : toRemove)
                    poolRemovedFuture.add(removePool(h));

                return Futures.successfulAsList(poolRemovedFuture);
            }
        });
    }

    void updateCreatedPools(Host h) {
        HostDistance dist = loadBalancingPolicy().distance(h);
        HostConnectionPool pool = pools.get(h);

        try {
            if (pool == null) {
                if (dist != HostDistance.IGNORED && h.state == Host.State.UP)
                    try {
                        maybeAddPool(h, null).get();
                    } catch (ExecutionException e) {
                        // Ignore, maybeAddPool has already handled the error
                    }
            } else if (dist != pool.hostDistance) {
                if (dist == HostDistance.IGNORED) {
                    removePool(h).get();
                } else {
                    pool.hostDistance = dist;
                    pool.ensureCoreConnections();
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            logger.error("Unexpected error while refreshing connection pools", e.getCause());
        }
    }

    void onDown(Host host) throws InterruptedException, ExecutionException {
        // Note that with well behaved balancing policy (that ignore dead nodes), the removePool call is not necessary
        // since updateCreatedPools should take care of it. But better protect against non well behaving policies.
        removePool(host).force().get();
        updateCreatedPools().get();
    }

    void onRemove(Host host) throws InterruptedException, ExecutionException {
        onDown(host);
    }

    Message.Request makeRequestMessage(Statement statement, ByteBuffer pagingState) {
        // We need the protocol version, which is only available once the cluster has initialized. Initialize the session to ensure this is the case.
        // init() locks, so avoid if we know we don't need it.
        if (!isInit)
            init();
        ProtocolVersion version = cluster.manager.protocolVersion();

        ConsistencyLevel consistency = statement.getConsistencyLevel();
        if (consistency == null)
            consistency = configuration().getQueryOptions().getConsistencyLevel();

        ConsistencyLevel serialConsistency = statement.getSerialConsistencyLevel();
        if (version.compareTo(ProtocolVersion.V3) < 0 && statement instanceof BatchStatement) {
            if (serialConsistency != null)
                throw new UnsupportedFeatureException(version, "Serial consistency on batch statements is not supported");
        } else if (serialConsistency == null)
            serialConsistency = configuration().getQueryOptions().getSerialConsistencyLevel();

        long defaultTimestamp = Long.MIN_VALUE;
        if (cluster.manager.protocolVersion().compareTo(ProtocolVersion.V3) >= 0) {
            defaultTimestamp = statement.getDefaultTimestamp();
            if (defaultTimestamp == Long.MIN_VALUE)
                defaultTimestamp = cluster.getConfiguration().getPolicies().getTimestampGenerator().next();
        }

        int fetchSize = statement.getFetchSize();
        ByteBuffer usedPagingState = pagingState;

        if (version == ProtocolVersion.V1) {
            assert pagingState == null;
            // We don't let the user change the fetchSize globally if the proto v1 is used, so we just need to
            // check for the case of a per-statement override
            if (fetchSize <= 0)
                fetchSize = -1;
            else if (fetchSize != Integer.MAX_VALUE)
                throw new UnsupportedFeatureException(version, "Paging is not supported");
        } else if (fetchSize <= 0) {
            fetchSize = configuration().getQueryOptions().getFetchSize();
        }

        if (fetchSize == Integer.MAX_VALUE)
            fetchSize = -1;

        if (pagingState == null) {
            usedPagingState = statement.getPagingState();
        }

        if (statement instanceof StatementWrapper)
            statement = ((StatementWrapper) statement).getWrappedStatement();

        if (statement instanceof RegularStatement) {
            RegularStatement rs = (RegularStatement) statement;

            // It saddens me that we special case for the query builder here, but for now this is simpler.
            // We could provide a general API in RegularStatement instead at some point but it's unclear what's
            // the cleanest way to do that is right now (and it's probably not really that useful anyway).
            if (version == ProtocolVersion.V1 && rs instanceof com.datastax.driver.core.querybuilder.BuiltStatement)
                ((com.datastax.driver.core.querybuilder.BuiltStatement) rs).setForceNoValues(true);

            ByteBuffer[] rawValues = rs.getValues(version);

            if (version == ProtocolVersion.V1 && rawValues != null)
                throw new UnsupportedFeatureException(version, "Binary values are not supported");

            List<ByteBuffer> values = rawValues == null ? Collections.<ByteBuffer>emptyList() : Arrays.asList(rawValues);
            String qString = rs.getQueryString();
            Requests.QueryProtocolOptions options = new Requests.QueryProtocolOptions(Message.Request.Type.QUERY, consistency, values, false,
                    fetchSize, usedPagingState, serialConsistency, defaultTimestamp);
            return new Requests.Query(qString, options, statement.isTracing());
        } else if (statement instanceof BoundStatement) {
            BoundStatement bs = (BoundStatement) statement;
            if (!cluster.manager.preparedQueries.containsKey(bs.statement.getPreparedId().id)) {
                throw new InvalidQueryException(String.format("Tried to execute unknown prepared query : %s. "
                        + "You may have used a PreparedStatement that was created with another Cluster instance.", bs.statement.getPreparedId().id));
            }
            bs.ensureAllSet();
            boolean skipMetadata = version != ProtocolVersion.V1 && bs.statement.getPreparedId().resultSetMetadata != null;
            Requests.QueryProtocolOptions options = new Requests.QueryProtocolOptions(Message.Request.Type.EXECUTE, consistency, Arrays.asList(bs.wrapper.values), skipMetadata,
                    fetchSize, usedPagingState, serialConsistency, defaultTimestamp);
            return new Requests.Execute(bs.statement.getPreparedId().id, options, statement.isTracing());
        } else {
            assert statement instanceof BatchStatement : statement;
            assert pagingState == null;

            if (version == ProtocolVersion.V1)
                throw new UnsupportedFeatureException(version, "Protocol level batching is not supported");

            BatchStatement bs = (BatchStatement) statement;
            bs.ensureAllSet();
            BatchStatement.IdAndValues idAndVals = bs.getIdAndValues(version);
            Requests.BatchProtocolOptions options = new Requests.BatchProtocolOptions(consistency, serialConsistency, defaultTimestamp);
            return new Requests.Batch(bs.batchType, idAndVals.ids, idAndVals.values, options, statement.isTracing());
        }
    }

    /**
     * Execute the provided request.
     * <p/>
     * This method will find a suitable node to connect to using the
     * {@link LoadBalancingPolicy} and handle host failover.
     */
    void execute(final RequestHandler.Callback callback, final Statement statement) {
        if (isInit)
            new RequestHandler(this, callback, statement).sendRequest();
        else
            this.initAsync().addListener(new Runnable() {
                @Override
                public void run() {
                    new RequestHandler(SessionManager.this, callback, statement).sendRequest();
                }
            }, executor());
    }

    private ListenableFuture<PreparedStatement> prepare(final PreparedStatement statement, InetSocketAddress toExclude) {
        final String query = statement.getQueryString();
        List<ListenableFuture<Response>> futures = Lists.newArrayListWithExpectedSize(pools.size());
        for (final Map.Entry<Host, HostConnectionPool> entry : pools.entrySet()) {
            if (entry.getKey().getSocketAddress().equals(toExclude))
                continue;

            try {
                // Preparing is not critical: if it fails, it will fix itself later when the user tries to execute
                // the prepared query. So don't block if no connection is available, simply abort.
                final Connection c = entry.getValue().borrowConnection(0, TimeUnit.MILLISECONDS);
                ListenableFuture<Response> future = c.write(new Requests.Prepare(query));
                Futures.addCallback(future, new FutureCallback<Response>() {
                    @Override
                    public void onSuccess(Response result) {
                        c.release();
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        logger.debug(String.format("Unexpected error while preparing query (%s) on %s", query, entry.getKey()), t);
                        c.release();
                    }
                });
                futures.add(future);
            } catch (Exception e) {
                // Again, not being able to prepare the query right now is no big deal, so just ignore
            }
        }
        // Return the statement when all futures are done
        return Futures.transform(
                Futures.successfulAsList(futures),
                Functions.constant(statement));
    }

    ResultSetFuture executeQuery(Message.Request msg, Statement statement) {
        DefaultResultSetFuture future = new DefaultResultSetFuture(this, configuration().getProtocolOptions().getProtocolVersionEnum(), msg);
        execute(future, statement);
        return future;
    }

    void cleanupIdleConnections(long now) {
        for (HostConnectionPool pool : pools.values()) {
            pool.cleanupIdleConnections(now);
        }
    }

    @Override
    protected void checkNotInEventLoop() {
        Connection.Factory connectionFactory = cluster.manager.connectionFactory;
        if (!CHECK_IO_DEADLOCKS || connectionFactory == null)
            return;
        for (EventExecutor executor : connectionFactory.eventLoopGroup) {
            if (executor.inEventLoop()) {
                throw new IllegalStateException(
                        "Detected a synchronous Session call (execute() or prepare()) on an I/O thread, " +
                                "this can cause deadlocks or unpredictable behavior. " +
                                "Make sure your Future callbacks only use async calls, or schedule them on a " +
                                "different executor.");
            }
        }
    }

    private static class State implements Session.State {

        private final SessionManager session;
        private final List<Host> connectedHosts;
        private final int[] openConnections;
        private final int[] trashedConnections;
        private final int[] inFlightQueries;

        private State(SessionManager session) {
            this.session = session;
            this.connectedHosts = ImmutableList.copyOf(session.pools.keySet());

            this.openConnections = new int[connectedHosts.size()];
            this.trashedConnections = new int[connectedHosts.size()];
            this.inFlightQueries = new int[connectedHosts.size()];

            int i = 0;
            for (Host h : connectedHosts) {
                HostConnectionPool p = session.pools.get(h);
                // It's possible we race and the host has been removed since the beginning of this
                // functions. In that case, the fact it's part of getConnectedHosts() but has no opened
                // connections will be slightly weird, but it's unlikely enough that we don't bother avoiding.
                if (p == null) {
                    openConnections[i] = 0;
                    trashedConnections[i] = 0;
                    inFlightQueries[i] = 0;
                    continue;
                }

                openConnections[i] = p.opened();
                inFlightQueries[i] = p.totalInFlight.get();
                trashedConnections[i] = p.trashed();
                i++;
            }
        }

        private int getIdx(Host h) {
            // We guarantee that we only ever create one Host object per-address, which means that '=='
            // comparison is a proper way to test Host equality. Given that, the number of hosts
            // per-session will always be small enough (even 1000 is kind of small and even with a 1000+
            // node cluster, you probably don't want a Session to connect to all of them) that iterating
            // over connectedHosts will never be much more inefficient than keeping a
            // Map<Host, SomeStructureForHostInfo>. And it's less garbage/memory consumption so...
            for (int i = 0; i < connectedHosts.size(); i++)
                if (h == connectedHosts.get(i))
                    return i;
            return -1;
        }

        @Override
        public Session getSession() {
            return session;
        }

        @Override
        public Collection<Host> getConnectedHosts() {
            return connectedHosts;
        }

        @Override
        public int getOpenConnections(Host host) {
            int i = getIdx(host);
            return i < 0 ? 0 : openConnections[i];
        }

        @Override
        public int getTrashedConnections(Host host) {
            int i = getIdx(host);
            return i < 0 ? 0 : trashedConnections[i];
        }

        @Override
        public int getInFlightQueries(Host host) {
            int i = getIdx(host);
            return i < 0 ? 0 : inFlightQueries[i];
        }
    }
}
