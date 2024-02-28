
package com.vortex.distr;

import com.vortex.vortexdb.VortexException;
import com.vortex.vortexdb.backend.serializer.SerializerFactory;
import com.vortex.vortexdb.backend.store.BackendProviderFactory;
import com.vortex.vortexdb.config.CoreOptions;
import com.vortex.common.config.VortexConfig;
import com.vortex.common.config.OptionSpace;
import com.vortex.vortexdb.plugin.VortexPlugin;
import com.vortex.common.util.E;
import com.vortex.common.util.Log;
import com.vortex.common.util.VersionUtil;
import com.vortex.vortexdb.version.CoreVersion;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;

import java.io.InputStream;
import java.util.List;
import java.util.ServiceLoader;

public class RegisterUtil {

    private static final Logger LOG = Log.logger(RegisterUtil.class);

    static {
        OptionSpace.register("core", CoreOptions.instance());
        OptionSpace.register("dist", DistOptions.instance());
    }

    public static void registerBackends() {
        String confFile = "/backend.properties";
        InputStream input = RegisterUtil.class.getClass()
                                        .getResourceAsStream(confFile);
        E.checkState(input != null,
                     "Can't read file '%s' as stream", confFile);

        PropertiesConfiguration props = new PropertiesConfiguration();
        props.setDelimiterParsingDisabled(true);
        try {
            props.load(input);
        } catch (ConfigurationException e) {
            throw new VortexException("Can't load config file: %s", e, confFile);
        }

        VortexConfig config = new VortexConfig(props);
        List<String> backends = config.get(DistOptions.BACKENDS);
        for (String backend : backends) {
            registerBackend(backend);
        }
    }

    private static void registerBackend(String backend) {
        switch (backend) {
            case "cassandra":
                registerCassandra();
                break;
            case "mysql":
                //registerMysql();
                break;
            case "postgresql":
                //registerPostgresql();
                break;
            default:
                throw new VortexException("Unsupported backend type '%s'",
                                        backend);
        }
    }


    // TODO: CREATE THIS BACKEND
    public static void registerCassandra() {
        // Register config
        OptionSpace.register("cassandra",
                "com.vortex.backend.store.cassandra.CassandraOptions");
        // Register serializer
        SerializerFactory.register("cassandra",
                "com.vortex.backend.store.cassandra.CassandraSerializer");
        // Register backend
        BackendProviderFactory.register("cassandra",
                "com.vortex.backend.store.cassandra.CassandraStoreProvider");
    }

    // TODO: CREATE THIS BACKEND
    public static void registerMysql() {
        // Register config
        OptionSpace.register("mysql",
                "com.vortex.vortexdb.backend.store.mysql.MysqlOptions");
        // Register serializer
        SerializerFactory.register("mysql",
                "com.vortex.vortexdb.backend.store.mysql.MysqlSerializer");
        // Register backend
        BackendProviderFactory.register("mysql",
                "com.vortex.vortexdb.backend.store.mysql.MysqlStoreProvider");
    }

    // TODO: CREATE THIS BACKEND
    public static void registerPostgresql() {
        // Register config
        OptionSpace.register("postgresql",
                "com.vortex.vortexdb.backend.store.postgresql.PostgresqlOptions");
        // Register serializer
        SerializerFactory.register("postgresql",
                "com.vortex.vortexdb.backend.store.postgresql.PostgresqlSerializer");
        // Register backend
        BackendProviderFactory.register("postgresql",
                "com.vortex.vortexdb.backend.store.postgresql.PostgresqlStoreProvider");
    }

    public static void registerServer() {
        // Register ServerOptions (rest-server)
        OptionSpace.register("server", "com.vortex.api.config.ServerOptions");
        // Register RpcOptions (rpc-server)
        OptionSpace.register("rpc", "com.vortex.rpc.config.RpcOptions");
        // Register AuthOptions (auth-server)
        OptionSpace.register("auth", "com.vortex.vortexdb.config.AuthOptions");
        // register CoreOprions (Core)
        OptionSpace.register("core", "com.vortex.vortexdb.config.CoreOptions");

        OptionSpace.register("cassandra",
                "com.vortex.backend.store.cassandra.CassandraOptions");

    }

    /**
     * TODO: fix this -> Scan the jars in plugins directory and load them
     */
    public static void registerPlugins() {
        ServiceLoader<VortexPlugin> plugins = ServiceLoader.load(
                                                 VortexPlugin.class);
        for (VortexPlugin plugin : plugins) {
            LOG.info("Loading plugin {}({})",
                     plugin.name(), plugin.getClass().getCanonicalName());
            String minVersion = plugin.supportsMinVersion();
            String maxVersion = plugin.supportsMaxVersion();

            if (!VersionUtil.match(CoreVersion.VERSION, minVersion,
                                   maxVersion)) {
                LOG.warn("Skip loading plugin '{}' due to the version range " +
                         "'[{}, {})' that it's supported doesn't cover " +
                         "current core version '{}'", plugin.name(),
                         minVersion, maxVersion, CoreVersion.VERSION.get());
                continue;
            }
            try {
                plugin.register();
                LOG.info("Loaded plugin '{}'", plugin.name());
            } catch (Exception e) {
                throw new VortexException("Failed to load plugin '%s'",
                                        plugin.name(), e);
            }
        }
    }
}
