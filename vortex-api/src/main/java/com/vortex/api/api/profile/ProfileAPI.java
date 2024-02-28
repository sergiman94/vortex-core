
package com.vortex.api.api.profile;

import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import java.util.*;
import com.vortex.common.util.E;
import com.vortex.common.util.InsertionOrderUtil;
import com.vortex.vortexdb.util.JsonUtil;
import com.vortex.vortexdb.version.CoreVersion;
import com.codahale.metrics.annotation.Timed;
import org.apache.commons.lang3.StringUtils;
import org.apache.tinkerpop.shaded.jackson.annotation.JsonProperty;
import org.glassfish.jersey.server.model.Parameter;
import org.glassfish.jersey.server.model.Parameter.Source;
import org.glassfish.jersey.server.model.Resource;
import org.glassfish.jersey.server.model.ResourceMethod;



@Path("/")
@Singleton
public class ProfileAPI {

    private static final String SERVICE = "vortex";
    private static final String DOC = "";
    private static final String API_DOC = DOC + "";

    private static String SERVER_PROFILES = null;
    private static String API_PROFILES = null;

    @GET
    @Timed
    @Produces(MediaType.APPLICATION_JSON)
    public String getProfile(@Context Application application) {
        // May init multi times by multi threads, but no effect on the results
        if (SERVER_PROFILES != null) {
            return SERVER_PROFILES;
        }

        Map<String, Object> profiles = InsertionOrderUtil.newMap();
        profiles.put("service", SERVICE);
        profiles.put("version", CoreVersion.VERSION.toString());
        profiles.put("doc", DOC);
        profiles.put("api_doc", API_DOC);
        Set<String> apis = new TreeSet<>();
        for (Class<?> clazz : application.getClasses()) {
            if (!isAnnotatedPathClass(clazz)) {
                continue;
            }
            Resource resource = Resource.from(clazz);
            APICategory apiCategory = APICategory.parse(resource.getName());
            apis.add(apiCategory.dir);
        }
        profiles.put("apis", apis);
        SERVER_PROFILES = JsonUtil.toJson(profiles);
        return SERVER_PROFILES;
    }

    @GET
    @Path("apis")
    @Timed
    @Produces(MediaType.APPLICATION_JSON)
    public String showAllAPIs(@Context Application application) {
        if (API_PROFILES != null) {
            return API_PROFILES;
        }

        APIProfiles apiProfiles = new APIProfiles();
        for (Class<?> clazz : application.getClasses()) {
            if (!isAnnotatedPathClass(clazz)) {
                continue;
            }

            Resource resource = Resource.from(clazz);
            APICategory apiCategory = APICategory.parse(resource.getName());

            String url = resource.getPath();
            // List all methods of this resource
            for (ResourceMethod rm : resource.getResourceMethods()) {
                APIProfile profile = APIProfile.parse(url, rm);
                apiProfiles.put(apiCategory, profile);
            }
            // List all methods of this resource's child resources
            for (Resource childResource : resource.getChildResources()) {
                String childUrl = url + "/" + childResource.getPath();
                for (ResourceMethod rm : childResource.getResourceMethods()) {
                    APIProfile profile = APIProfile.parse(childUrl, rm);
                    apiProfiles.put(apiCategory, profile);
                }
            }
        }
        API_PROFILES = JsonUtil.toJson(apiProfiles);
        return API_PROFILES;
    }

    private static boolean isAnnotatedPathClass(Class<?> clazz) {
        if (clazz.isAnnotationPresent(Path.class)) {
            return true;
        }
        for (Class<?> i : clazz.getInterfaces()) {
            if (i.isAnnotationPresent(Path.class)) {
                return true;
            }
        }
        return false;
    }

    private static class APIProfiles {

        @JsonProperty("apis")
        private final Map<String, Map<String, List<APIProfile>>> apis;

        public APIProfiles() {
            this.apis = new TreeMap<>();
        }

        public void put(APICategory category, APIProfile profile) {
            Map<String, List<APIProfile>> categories;
            categories = this.apis.computeIfAbsent(category.dir,
                                                   k -> new TreeMap<>());
            List<APIProfile> profiles = categories.computeIfAbsent(
                                                   category.category,
                                                   k -> new ArrayList<>());
            profiles.add(profile);
        }
    }

    private static class APIProfile {

        @JsonProperty("url")
        private final String url;
        @JsonProperty("method")
        private final String method;
        @JsonProperty("parameters")
        private final List<ParamInfo> parameters;

        public APIProfile(String url, String method,
                          List<ParamInfo> parameters) {
            this.url = url;
            this.method = method;
            this.parameters = parameters;
        }

        public static APIProfile parse(String url, ResourceMethod resource) {
            String method = resource.getHttpMethod();
            List<ParamInfo> params = new ArrayList<>();
            for (Parameter param : resource.getInvocable().getParameters()) {
                if (param.getSource() == Source.QUERY) {
                    String name = param.getSourceName();
                    String type = param.getType().getTypeName();
                    String defaultValue = param.getDefaultValue();
                    params.add(new ParamInfo(name, type, defaultValue));
                } else if (param.getSource() == Source.ENTITY) {
                    String type = param.getType().getTypeName();
                    params.add(new ParamInfo("body", type));
                }
            }
            return new APIProfile(url, method, params);
        }

        private static class ParamInfo {

            @JsonProperty("name")
            private final String name;
            @JsonProperty("type")
            private final String type;
            @JsonProperty("default_value")
            private final String defaultValue;

            public ParamInfo(String name, String type) {
                this(name, type, null);
            }

            public ParamInfo(String name, String type, String defaultValue) {
                this.name = name;
                this.type = type;
                this.defaultValue = defaultValue;
            }
        }
    }

    private static class APICategory {

        private final String dir;
        private final String category;

        public APICategory(String dir, String category) {
            this.dir = dir;
            this.category = category;
        }

        public static APICategory parse(String fullName) {
            String[] parts = StringUtils.split(fullName, ".");
            E.checkState(parts.length >= 2, "Invalid api name");
            String dir = parts[parts.length - 2];
            String category = parts[parts.length - 1];
            return new APICategory(dir, category);
        }
    }
}
