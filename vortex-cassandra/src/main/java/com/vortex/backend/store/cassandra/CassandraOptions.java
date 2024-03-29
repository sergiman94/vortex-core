package com.vortex.backend.store.cassandra;

import com.vortex.common.config.ConfigListOption;
import com.vortex.common.config.ConfigOption;
import com.vortex.common.config.OptionHolder;

import static com.vortex.common.config.OptionChecker.*;

public class CassandraOptions extends OptionHolder {

    private CassandraOptions() {
        super();
    }

    private static volatile CassandraOptions instance;

    public static synchronized CassandraOptions instance() {
        if (instance == null) {
            instance = new CassandraOptions();
            instance.registerOptions();
        }
        return instance;
    }

    public static final ConfigOption<String> CASSANDRA_HOST =
            new ConfigOption<>(
                    "cassandra.host",
                    "The seeds hostname or ip address of cassandra cluster.",
                    disallowEmpty(),
                    "127.17.0.2"
            );

    public static final ConfigOption<String> CASSANDRA_PORT =
            new ConfigOption<>(
                    "cassandra.port",
                    "The seeds port address of cassandra cluster.",
                    //rangeInt(1, 65535),
                    "9042"
            );

    public static final ConfigOption<String> CASSANDRA_USERNAME =
            new ConfigOption<>(
                    "cassandra.username",
                    "The username to use to login to cassandra cluster.",
                    ""
            );

    public static final ConfigOption<String> CASSANDRA_PASSWORD =
            new ConfigOption<>(
                    "cassandra.password",
                    "The password corresponding to cassandra.username.",
                    ""
            );

    public static final ConfigOption<Integer> CASSANDRA_CONN_TIMEOUT =
            new ConfigOption<>(
                    "cassandra.connect_timeout",
                    "The cassandra driver connect server timeout(seconds).",
                    rangeInt(1, 30),
                    5
            );

    public static final ConfigOption<Integer> CASSANDRA_READ_TIMEOUT =
            new ConfigOption<>(
                    "cassandra.read_timeout",
                    "The cassandra driver read from server timeout(seconds).",
                    rangeInt(1, 120),
                    20
            );

    public static final ConfigOption<String> CASSANDRA_STRATEGY =
            new ConfigOption<>(
                    "cassandra.keyspace.strategy",
                    "The replication strategy of keyspace, valid value is " +
                    "SimpleStrategy or NetworkTopologyStrategy.",
                    allowValues("SimpleStrategy", "NetworkTopologyStrategy"),
                    "SimpleStrategy"
            );

    public static final ConfigListOption<String> CASSANDRA_REPLICATION =
            new ConfigListOption<>(
                    "cassandra.keyspace.replication",
                    "The keyspace replication factor of SimpleStrategy, " +
                    "like '[3]'. Or replicas in each datacenter of " +
                    "NetworkTopologyStrategy, like '[dc1:2,dc2:1]'.",
                    disallowEmpty(),
                    "3"
            );

    public static final ConfigOption<String> CASSANDRA_COMPRESSION =
            new ConfigOption<>(
                    "cassandra.compression_type",
                    "The compression algorithm of cassandra transport: none/snappy/lz4.",
                    allowValues("none", "snappy", "lz4"),
                    "none"
            );

    public static final ConfigOption<Integer> CASSANDRA_JMX_PORT =
            new ConfigOption<>(
                    "cassandra.jmx_port",
                    "The port of JMX API service for cassandra",
                    rangeInt(1, 65535),
                    7199
            );

    public static final ConfigOption<Integer> AGGR_TIMEOUT =
            new ConfigOption<>(
                    "cassandra.aggregation_timeout",
                    "The timeout in seconds of waiting for aggregation.",
                    positiveInt(),
                    12 * 60 * 60
            );
}
