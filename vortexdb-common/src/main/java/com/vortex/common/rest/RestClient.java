
package com.vortex.common.rest;

import javax.ws.rs.core.MultivaluedMap;

import java.util.Map;

public interface RestClient {

    public RestResult post(String path, Object object);
    public RestResult post(String path, Object object,
                           MultivaluedMap<String, Object> headers);
    public RestResult post(String path, Object object,
                           Map<String, Object> params);
    public RestResult post(String path, Object object,
                           MultivaluedMap<String, Object> headers,
                           Map<String, Object> params);

    public RestResult put(String path, String id, Object object);
    public RestResult put(String path, String id, Object object,
                          MultivaluedMap<String, Object> headers);
    public RestResult put(String path, String id, Object object,
                          Map<String, Object> params);
    public RestResult put(String path, String id, Object object,
                          MultivaluedMap<String, Object> headers,
                          Map<String, Object> params);

    public RestResult get(String path);
    public RestResult get(String path, Map<String, Object> params);
    public RestResult get(String path, String id);

    public RestResult delete(String path, Map<String, Object> params);
    public RestResult delete(String path, String id);

    public void close();
}
