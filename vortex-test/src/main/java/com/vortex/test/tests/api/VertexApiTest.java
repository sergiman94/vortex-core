
package com.vortex.test.tests.api;

import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.core.Response;
import java.io.IOException;

public class VertexApiTest extends BaseApiTest {

    private static String path = "/graphs/vortex/graph/vertices/";

    @Before
    public void prepareSchema() {
        BaseApiTest.initPropertyKey();
        BaseApiTest.initVertexLabel();
    }

    @Test
    public void testCreate() {
        String vertex = "{"
                + "\"label\":\"person\","
                + "\"properties\":{"
                + "\"name\":\"James\","
                + "\"city\":\"Beijing\","
                + "\"age\":19}"
                + "}";
        Response r = client().post(path, vertex);
        assertResponseStatus(201, r);
    }

    @Test
    public void testGet() throws IOException {
        String vertex = "{"
                + "\"label\":\"person\","
                + "\"properties\":{"
                + "\"name\":\"James\","
                + "\"city\":\"Beijing\","
                + "\"age\":19}"
                + "}";
        Response r = client().post(path, vertex);
        String content = assertResponseStatus(201, r);

        String id = parseId(content);
        id = String.format("\"%s\"", id);
        r = client().get(path, id);
        assertResponseStatus(200, r);
    }

    @Test
    public void testList() {
        String vertex = "{"
                + "\"label\":\"person\","
                + "\"properties\":{"
                + "\"name\":\"James\","
                + "\"city\":\"Beijing\","
                + "\"age\":19}"
                + "}";
        Response r = client().post(path, vertex);
        assertResponseStatus(201, r);

        r = client().get(path);
        assertResponseStatus(200, r);
    }

    @Test
    public void testDelete() throws IOException {
        String vertex = "{"
                + "\"label\":\"person\","
                + "\"properties\":{"
                + "\"name\":\"James\","
                + "\"city\":\"Beijing\","
                + "\"age\":19}"
                + "}";
        Response r = client().post(path, vertex);
        String content = assertResponseStatus(201, r);

        String id = parseId(content);
        id = String.format("\"%s\"", id);
        r = client().delete(path, id);
        assertResponseStatus(204, r);
    }
}
