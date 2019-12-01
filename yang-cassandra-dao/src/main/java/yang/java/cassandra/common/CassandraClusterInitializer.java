package yang.java.cassandra.common;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.*;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import yang.java.cassandra.common.impl.InitialListener;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class CassandraClusterInitializer implements Cluster.Initializer {

    private String clusterName;
    private final List<InetSocketAddress> contactPoints = Lists.newArrayList();
    private Configuration configuration;
    private Collection<Host.StateListener> initialListeners;


    public CassandraClusterInitializer(String clusterName, String contactPointsStr) {
        this.clusterName = clusterName;
        contactPoints.addAll(Splitter.on(';').splitToList(contactPointsStr)
                .stream()
                .map(this::mapToInetSocketAddress)
                .collect(Collectors.toList()));
        initConfiguration();
        initialListeners = Lists.newArrayList(new InitialListener());
    }

    private InetSocketAddress mapToInetSocketAddress(String str) {
        String[] arrgas = str.split(":", 2);
        return new InetSocketAddress(arrgas[0], Integer.parseInt(arrgas[1]));
    }

    private void initConfiguration() {
        Policies policies = Policies.builder()
                .withLoadBalancingPolicy(new TokenAwarePolicy(DCAwareRoundRobinPolicy.builder().withLocalDc("datacenter1")
                        .build()))
                .withAddressTranslator(new IdentityTranslator())
                .withReconnectionPolicy(new ExponentialReconnectionPolicy(1000L, 70000L))
                .withTimestampGenerator(ServerSideTimestampGenerator.INSTANCE)
                .withRetryPolicy(DefaultRetryPolicy.INSTANCE)
                .withSpeculativeExecutionPolicy(new ConstantSpeculativeExecutionPolicy(1000L, 3))
                .build();

        ProtocolOptions protocolOptions = new ProtocolOptions();
        PoolingOptions poolingOptions = new PoolingOptions();
        poolingOptions.setConnectionsPerHost(HostDistance.LOCAL, 2, 10)
                .setConnectionsPerHost(HostDistance.REMOTE, 2, 10)
                .setMaxRequestsPerConnection(HostDistance.LOCAL, 1000)
                .setMaxRequestsPerConnection(HostDistance.REMOTE, 1000)
                .setPoolTimeoutMillis(10000000);
        SocketOptions socketOptions = new SocketOptions().setReadTimeoutMillis(1000000);
        QueryOptions queryOptions = new QueryOptions().setConsistencyLevel(ConsistencyLevel.LOCAL_ONE)
                .setPrepareOnAllHosts(true);
        MetricsOptions metricsOptions = new MetricsOptions();

        Configuration configuration = Configuration.builder()
        .withMetricsOptions(metricsOptions)
                .withPolicies(policies)
                .withSocketOptions(socketOptions)
                .withPoolingOptions(poolingOptions)
                .withQueryOptions(queryOptions)
                .build();
        this.configuration = configuration;
    }

    public String getClusterName() {
        return clusterName;
    }

    public List<InetSocketAddress> getContactPoints() {
        return contactPoints;
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public Collection<Host.StateListener> getInitialListeners() {
        return initialListeners;
    }
}
