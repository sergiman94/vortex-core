
package com.vortex.test.tests.unit;

import com.vortex.common.testutil.Assert;
import com.vortex.vortexdb.Vortex;
import com.vortex.vortexdb.VortexException;
import com.vortex.vortexdb.VortexFactory;
import com.vortex.vortexdb.backend.id.IdGenerator;
import com.vortex.vortexdb.backend.store.ram.RamTable;
import com.vortex.vortexdb.schema.EdgeLabel;
import com.vortex.vortexdb.schema.SchemaManager;
import com.vortex.vortexdb.schema.VertexLabel;
import com.vortex.vortexdb.structure.VortexEdge;
import com.vortex.vortexdb.structure.VortexVertex;
import com.vortex.vortexdb.type.define.Directions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Iterator;

public class RamTableTest {

    // max value is 4 billion
    private static final int VERTEX_SIZE = 10000000;
    private static final int EDGE_SIZE = 20000000;

    private Vortex graph;

    @Before
    public void setup() {
        this.graph = VortexFactory.open(FakeObjects.newConfig());
        SchemaManager schema = this.graph.schema();

        schema.propertyKey("p3").asText().create();

        schema.vertexLabel("vl1").useCustomizeNumberId().create();
        schema.vertexLabel("vl2").useCustomizeNumberId().create();
        schema.vertexLabel("vl3").useCustomizeStringId().create();

        schema.edgeLabel("el1")
              .sourceLabel("vl1")
              .targetLabel("vl1")
              .create();
        schema.edgeLabel("el2")
              .sourceLabel("vl2")
              .targetLabel("vl2")
              .create();
        schema.edgeLabel("el3")
              .sourceLabel("vl3")
              .targetLabel("vl3")
              .properties("p3")
              .multiTimes()
              .sortKeys("p3")
              .create();
    }

    @After
    public void teardown() throws Exception {
        this.graph.close();
    }

    private Vortex graph() {
        return this.graph;
    }

    @Test
    public void testAddAndQuery() throws Exception {
        Vortex graph = this.graph();
        int el1 = (int) graph.edgeLabel("el1").id().asLong();
        int el2 = (int) graph.edgeLabel("el2").id().asLong();

        RamTable table = new RamTable(graph, VERTEX_SIZE, EDGE_SIZE);
        long oldSize = table.edgesSize();
        // insert edges
        for (int i = 0; i < VERTEX_SIZE; i++) {
            table.addEdge(true, i, i, Directions.OUT, el1);
            Assert.assertEquals(oldSize + 2 * i + 1, table.edgesSize());

            table.addEdge(false, i, i + 1, Directions.IN, el2);
            Assert.assertEquals(oldSize + 2 * i + 2, table.edgesSize());
        }

        // query by BOTH
        for (int i = 0; i < VERTEX_SIZE; i++) {
            Iterator<  VortexEdge> edges = table.query(i, Directions.BOTH, 0);

            Assert.assertTrue(edges.hasNext());
            VortexEdge edge1 = edges.next();
            Assert.assertEquals(i, edge1.id().ownerVertexId().asLong());
            Assert.assertEquals(i, edge1.id().otherVertexId().asLong());
            Assert.assertEquals(Directions.OUT, edge1.direction());
            Assert.assertEquals("el1", edge1.label());

            Assert.assertTrue(edges.hasNext());
            VortexEdge edge2 = edges.next();
            Assert.assertEquals(i, edge2.id().ownerVertexId().asLong());
            Assert.assertEquals(i + 1L, edge2.id().otherVertexId().asLong());
            Assert.assertEquals(Directions.IN, edge2.direction());
            Assert.assertEquals("el2", edge2.label());

            Assert.assertFalse(edges.hasNext());
        }
        // query by OUT
        for (int i = 0; i < VERTEX_SIZE; i++) {
            Iterator<  VortexEdge> edges = table.query(i, Directions.OUT, el1);

            Assert.assertTrue(edges.hasNext());
            VortexEdge edge1 = edges.next();
            Assert.assertEquals(i, edge1.id().ownerVertexId().asLong());
            Assert.assertEquals(i, edge1.id().otherVertexId().asLong());
            Assert.assertEquals(Directions.OUT, edge1.direction());
            Assert.assertEquals("el1", edge1.label());

            Assert.assertFalse(edges.hasNext());
        }
        // query by IN
        for (int i = 0; i < VERTEX_SIZE; i++) {
            Iterator<  VortexEdge> edges = table.query(i, Directions.IN, el2);

            Assert.assertTrue(edges.hasNext());
            VortexEdge edge1 = edges.next();
            Assert.assertEquals(i, edge1.id().ownerVertexId().asLong());
            Assert.assertEquals(i + 1L, edge1.id().otherVertexId().asLong());
            Assert.assertEquals(Directions.IN, edge1.direction());
            Assert.assertEquals("el2", edge1.label());

            Assert.assertFalse(edges.hasNext());
        }

        // query by BOTH & label 1
        for (int i = 0; i < VERTEX_SIZE; i++) {
            Iterator<  VortexEdge> edges = table.query(i, Directions.BOTH, el1);

            Assert.assertTrue(edges.hasNext());
            VortexEdge edge1 = edges.next();
            Assert.assertEquals(i, edge1.id().ownerVertexId().asLong());
            Assert.assertEquals(i, edge1.id().otherVertexId().asLong());
            Assert.assertEquals(Directions.OUT, edge1.direction());
            Assert.assertEquals("el1", edge1.label());

            Assert.assertFalse(edges.hasNext());
        }
        // query by BOTH & label 2
        for (int i = 0; i < VERTEX_SIZE; i++) {
            Iterator<  VortexEdge> edges = table.query(i, Directions.BOTH, el2);

            Assert.assertTrue(edges.hasNext());
            VortexEdge edge1 = edges.next();
            Assert.assertEquals(i, edge1.id().ownerVertexId().asLong());
            Assert.assertEquals(i + 1L, edge1.id().otherVertexId().asLong());
            Assert.assertEquals(Directions.IN, edge1.direction());
            Assert.assertEquals("el2", edge1.label());

            Assert.assertFalse(edges.hasNext());
        }

        // query non-exist vertex
        Iterator<  VortexEdge> edges = table.query(VERTEX_SIZE, Directions.BOTH, 0);
        Assert.assertFalse(edges.hasNext());
    }

    @Test
    public void testAddAndQueryWithoutAdjEdges() throws Exception {
        Vortex graph = this.graph();
        int el1 = (int) graph.edgeLabel("el1").id().asLong();
        int el2 = (int) graph.edgeLabel("el2").id().asLong();

        RamTable table = new RamTable(graph, VERTEX_SIZE, EDGE_SIZE);
        long oldSize = table.edgesSize();
        // insert edges
        for (int i = 0; i < VERTEX_SIZE; i++) {
            if (i % 3 != 0) {
                // don't insert edges for 2/3 vertices
                continue;
            }

            table.addEdge(true, i, i, Directions.OUT, el1);
            Assert.assertEquals(oldSize + i + 1, table.edgesSize());

            table.addEdge(false, i, i, Directions.OUT, el2);
            Assert.assertEquals(oldSize + i + 2, table.edgesSize());

            table.addEdge(false, i, i + 1, Directions.IN, el2);
            Assert.assertEquals(oldSize + i + 3, table.edgesSize());
        }

        // query by BOTH
        for (int i = 0; i < VERTEX_SIZE; i++) {
            Iterator<  VortexEdge> edges = table.query(i, Directions.BOTH, 0);

            if (i % 3 != 0) {
                Assert.assertFalse(edges.hasNext());
                continue;
            }

            Assert.assertTrue(edges.hasNext());
            VortexEdge edge1 = edges.next();
            Assert.assertEquals(i, edge1.id().ownerVertexId().asLong());
            Assert.assertEquals(i, edge1.id().otherVertexId().asLong());
            Assert.assertEquals(Directions.OUT, edge1.direction());
            Assert.assertEquals("el1", edge1.label());

            Assert.assertTrue(edges.hasNext());
            VortexEdge edge2 = edges.next();
            Assert.assertEquals(i, edge2.id().ownerVertexId().asLong());
            Assert.assertEquals(i, edge2.id().otherVertexId().asLong());
            Assert.assertEquals(Directions.OUT, edge2.direction());
            Assert.assertEquals("el2", edge2.label());

            Assert.assertTrue(edges.hasNext());
            VortexEdge edge3 = edges.next();
            Assert.assertEquals(i, edge3.id().ownerVertexId().asLong());
            Assert.assertEquals(i + 1L, edge3.id().otherVertexId().asLong());
            Assert.assertEquals(Directions.IN, edge3.direction());
            Assert.assertEquals("el2", edge3.label());

            Assert.assertFalse(edges.hasNext());
        }
    }

    @Test
    public void testAddInvalidVertexOrEdge() {
        Vortex graph = this.graph();
        VertexLabel vl3 = graph.vertexLabel("vl3");
        EdgeLabel el3 = graph.edgeLabel("el3");

        VertexLabel vl2 = graph.vertexLabel("vl2");
        EdgeLabel el2 = graph.edgeLabel("el2");

        RamTable table = new RamTable(graph, VERTEX_SIZE, EDGE_SIZE);

        VortexVertex ownerVertex = new VortexVertex(graph, IdGenerator.of(1), vl3);
        VortexEdge edge1 =   VortexEdge.constructEdge(ownerVertex, true, el3, "marko",
                                               IdGenerator.of(2));
        Assert.assertThrows(VortexException.class, () -> {
            table.addEdge(true, edge1);
        }, e -> {
            Assert.assertContains("Only edge label without sortkey is " +
                                  "supported by ramtable, but got 'el3(id=3)'",
                                  e.getMessage());
        });

        VortexVertex v1 = new VortexVertex(graph, IdGenerator.of("s1"), vl2);
        VortexEdge edge2 =   VortexEdge.constructEdge(v1, true, el2, "marko",
                                                IdGenerator.of("s2"));
        Assert.assertThrows(VortexException.class, () -> {
            table.addEdge(true, edge2);
        }, e -> {
            Assert.assertContains("Only number id is supported by ramtable, " +
                                  "but got string id 's1'", e.getMessage());
        });

        VortexVertex v2 = new VortexVertex(graph, IdGenerator.of(2), vl2);
        VortexEdge edge3 =   VortexEdge.constructEdge(v2, true, el2, "marko",
                                                IdGenerator.of("s2"));
        Assert.assertThrows(VortexException.class, () -> {
            table.addEdge(true, edge3);
        }, e -> {
            Assert.assertContains("Only number id is supported by ramtable, " +
                                  "but got string id 's2'", e.getMessage());
        });
    }
}
