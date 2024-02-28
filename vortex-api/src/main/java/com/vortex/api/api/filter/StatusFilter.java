
package com.vortex.api.api.filter;

import javax.ws.rs.NameBinding;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.ext.Provider;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Provider
public class StatusFilter implements ContainerResponseFilter {

    @Override
    public void filter(ContainerRequestContext requestContext,
                       ContainerResponseContext responseContext)
                       throws IOException {

        responseContext.getHeaders().putSingle("Access-Control-Allow-Origin", "*");
        responseContext.getHeaders().putSingle("Access-Control-Allow-Credentials", "true");
        responseContext.getHeaders().putSingle("Access-Control-Allow-Methods", "GET, POST, DELETE, PUT");
        responseContext.getHeaders().putSingle("Access-Control-Allow-Headers", "Content-Type, Accept");

        if (responseContext.getStatus() == 200) {
            for (Annotation i : responseContext.getEntityAnnotations()) {
                if (i instanceof Status) {
                    responseContext.setStatus(((Status) i).value());
                    break;
                }
            }
        }
    }

    @NameBinding
    @Retention(RetentionPolicy.RUNTIME)
    public @interface Status {
        int OK = 200;
        int CREATED = 201;
        int ACCEPTED = 202;

        int value();
    }
}
