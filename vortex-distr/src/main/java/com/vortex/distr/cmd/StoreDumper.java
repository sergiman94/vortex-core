package com.vortex.distr.cmd;

import com.vortex.vortexdb.VortexFactory;
import com.vortex.vortexdb.Vortex;
import com.vortex.vortexdb.backend.query.Query;
import com.vortex.vortexdb.backend.store.BackendEntry;
import com.vortex.vortexdb.backend.store.BackendStore;
import com.vortex.distr.RegisterUtil;
import com.vortex.common.testutil.Whitebox;
import com.vortex.vortexdb.type.VortexType;
import com.vortex.common.util.E;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;

import java.util.Iterator;

public class StoreDumper {

    private final Vortex graph;

    public StoreDumper(String conf) {
        this.graph = VortexFactory.open(conf);
    }

    public void dump(VortexType table, long offset, long limit) {
        BackendStore store = this.backendStore(table);

        Query query = new Query(table);
        Iterator<BackendEntry> rs = store.query(query);
        for (long i = 0; i < offset && rs.hasNext(); i++) {
            rs.next();
        }
        String title = String.format("Dump table %s (offset %d limit %d):",
                                     table, offset, limit);
        System.out.println(title);
        for (long i = 0; i < limit && rs.hasNext(); i++) {
            BackendEntry entry = rs.next();
            System.out.println(entry);
        }

        CloseableIterator.closeIterator(rs);
    }

    private BackendStore backendStore(VortexType table) {
        String m = table.isSchema() ? "schemaTransaction" : "graphTransaction";
        Object tx = Whitebox.invoke(this, "graph", m);
        return Whitebox.invoke(tx.getClass(), "store", tx);
    }

    public void close() throws Exception {
        this.graph.close();
    }

    public static void main(String[] args) throws Exception {
        E.checkArgument(args.length >= 1,
                        "StoreDumper need a config file.");

        String conf = args[0];
        RegisterUtil.registerBackends();

        VortexType table = VortexType.valueOf(arg(args, 1, "VERTEX").toUpperCase());
        long offset = Long.parseLong(arg(args, 2, "0"));
        long limit = Long.parseLong(arg(args, 3, "20"));

        StoreDumper dumper = new StoreDumper(conf);
        dumper.dump(table, offset, limit);
        dumper.close();

        // Stop daemon thread
        VortexFactory.shutdown(30L);
    }

    private static String arg(String[] args, int index, String deflt) {
        if (index < args.length) {
            return args[index];
        }
        return deflt;
    }
}
