
package com.vortex.vortexdb.auth;

import com.vortex.common.util.Log;
import com.vortex.vortexdb.VortexException;
import com.vortex.vortexdb.VortexParams;
import com.vortex.vortexdb.auth.VortexUser.P;
import com.vortex.vortexdb.auth.SchemaDefine.AuthElement;
import com.vortex.vortexdb.backend.cache.Cache;
import com.vortex.vortexdb.backend.cache.CacheManager;
import com.vortex.vortexdb.backend.id.Id;
import com.vortex.vortexdb.backend.id.IdGenerator;
import com.vortex.vortexdb.config.AuthOptions;
import com.vortex.common.config.VortexConfig;
import com.vortex.common.event.EventListener;
import com.vortex.vortexdb.type.define.Directions;
import com.vortex.common.util.*;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.vortex.vortexdb.util.Events;
import com.vortex.vortexdb.util.LockUtil;
import com.vortex.vortexdb.util.StringEncoding;
import io.jsonwebtoken.Claims;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;

import javax.security.sasl.AuthenticationException;
import javax.ws.rs.ForbiddenException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.Callable;

public class StandardAuthManager implements AuthManager {

    protected static final Logger LOG = Log.logger(StandardAuthManager.class);

    private final VortexParams graph;
    private final EventListener eventListener;

    // Cache <username, VortexUser>
    private final Cache<Id, VortexUser> usersCache;
    // Cache <userId, passwd>
    private final Cache<Id, String> pwdCache;
    // Cache <token, username>
    private final Cache<Id, String> tokenCache;

    private final EntityManager<VortexUser> users;
    private final EntityManager<VortexGroup> groups;
    private final EntityManager<VortexTarget> targets;
    private final EntityManager<VortexProject> project;

    private final RelationshipManager<VortexBelong> belong;
    private final RelationshipManager<VortexAccess> access;

    private final TokenGenerator tokenGenerator;
    private final long tokenExpire;

    public StandardAuthManager(VortexParams graph) {
        E.checkNotNull(graph, "graph");
        VortexConfig config = graph.configuration();
        long expired = config.get(AuthOptions.AUTH_CACHE_EXPIRE);
        long capacity = config.get(AuthOptions.AUTH_CACHE_CAPACITY);
        this.tokenExpire = config.get(AuthOptions.AUTH_TOKEN_EXPIRE) * 1000;

        this.graph = graph;
        this.eventListener = this.listenChanges();
        this.usersCache = this.cache("users", capacity, expired);
        this.pwdCache = this.cache("users_pwd", capacity, expired);
        this.tokenCache = this.cache("token", capacity, expired);

        this.users = new EntityManager<>(this.graph, VortexUser.P.USER,
                                         VortexUser::fromVertex);
        this.groups = new EntityManager<>(this.graph, VortexGroup.P.GROUP,
                                          VortexGroup::fromVertex);
        this.targets = new EntityManager<>(this.graph, VortexTarget.P.TARGET,
                                           VortexTarget::fromVertex);
        this.project = new EntityManager<>(this.graph, VortexProject.P.PROJECT,
                                           VortexProject::fromVertex);

        this.belong = new RelationshipManager<>(this.graph, VortexBelong.P.BELONG,
                                                VortexBelong::fromEdge);
        this.access = new RelationshipManager<>(this.graph, VortexAccess.P.ACCESS,
                                                VortexAccess::fromEdge);

        this.tokenGenerator = new TokenGenerator(config);
    }

    private <V> Cache<Id, V> cache(String prefix, long capacity,
                                   long expiredTime) {
        String name = prefix + "-" + this.graph.name();
        Cache<Id, V> cache = CacheManager.instance().cache(name, capacity);
        if (expiredTime > 0L) {
            cache.expire(Duration.ofSeconds(expiredTime).toMillis());
        } else {
            cache.expire(expiredTime);
        }
        return cache;
    }

    private EventListener listenChanges() {
        // Listen store event: "store.inited"
        Set<String> storeEvents = ImmutableSet.of(Events.STORE_INITED);
        EventListener eventListener = event -> {
            // Ensure user schema create after system info initialized
            if (storeEvents.contains(event.getName())) {
                try {
                    this.initSchemaIfNeeded();
                } finally {
                    this.graph.closeTx();
                }
                return true;
            }
            return false;
        };
        this.graph.loadSystemStore().provider().listen(eventListener);
        return eventListener;
    }

    private void unlistenChanges() {
        this.graph.loadSystemStore().provider().unlisten(this.eventListener);
    }

    @Override
    public boolean close() {
        this.unlistenChanges();
        return true;
    }

    private void initSchemaIfNeeded() {
        this.invalidateUserCache();
        VortexUser.schema(this.graph).initSchemaIfNeeded();
        VortexGroup.schema(this.graph).initSchemaIfNeeded();
        VortexTarget.schema(this.graph).initSchemaIfNeeded();
        VortexBelong.schema(this.graph).initSchemaIfNeeded();
        VortexAccess.schema(this.graph).initSchemaIfNeeded();
        VortexProject.schema(this.graph).initSchemaIfNeeded();
    }

    private void invalidateUserCache() {
        this.usersCache.clear();
    }

    private void invalidatePasswordCache(Id id) {
        this.pwdCache.invalidate(id);
        // Clear all tokenCache because can't get userId in it
        this.tokenCache.clear();
    }

    @Override
    public Id createUser(VortexUser user) {
        this.invalidateUserCache();
        return this.users.add(user);
    }

    @Override
    public Id updateUser(VortexUser user) {
        this.invalidateUserCache();
        this.invalidatePasswordCache(user.id());
        return this.users.update(user);
    }

    @Override
    public VortexUser deleteUser(Id id) {
        this.invalidateUserCache();
        this.invalidatePasswordCache(id);
        return this.users.delete(id);
    }

    @Override
    public VortexUser findUser(String name) {
        Id username = IdGenerator.of(name);
        VortexUser user = this.usersCache.get(username);
        if (user != null) {
            return user;
        }

        List<VortexUser> users = this.users.query(P.NAME, name, 2L);
        if (users.size() > 0) {
            assert users.size() == 1;
            user = users.get(0);
            this.usersCache.update(username, user);
        }
        return user;
    }

    @Override
    public VortexUser getUser(Id id) {
        return this.users.get(id);
    }

    @Override
    public List<VortexUser> listUsers(List<Id> ids) {
        return this.users.list(ids);
    }

    @Override
    public List<VortexUser> listAllUsers(long limit) {
        return this.users.list(limit);
    }

    @Override
    public Id createGroup(VortexGroup group) {
        this.invalidateUserCache();
        return this.groups.add(group);
    }

    @Override
    public Id updateGroup(VortexGroup group) {
        this.invalidateUserCache();
        return this.groups.update(group);
    }

    @Override
    public VortexGroup deleteGroup(Id id) {
        this.invalidateUserCache();
        return this.groups.delete(id);
    }

    @Override
    public VortexGroup getGroup(Id id) {
        return this.groups.get(id);
    }

    @Override
    public List<VortexGroup> listGroups(List<Id> ids) {
        return this.groups.list(ids);
    }

    @Override
    public List<VortexGroup> listAllGroups(long limit) {
        return this.groups.list(limit);
    }

    @Override
    public Id createTarget(VortexTarget target) {
        this.invalidateUserCache();
        return this.targets.add(target);
    }

    @Override
    public Id updateTarget(VortexTarget target) {
        this.invalidateUserCache();
        return this.targets.update(target);
    }

    @Override
    public VortexTarget deleteTarget(Id id) {
        this.invalidateUserCache();
        return this.targets.delete(id);
    }

    @Override
    public VortexTarget getTarget(Id id) {
        return this.targets.get(id);
    }

    @Override
    public List<VortexTarget> listTargets(List<Id> ids) {
        return this.targets.list(ids);
    }

    @Override
    public List<VortexTarget> listAllTargets(long limit) {
        return this.targets.list(limit);
    }

    @Override
    public Id createBelong(VortexBelong belong) {
        this.invalidateUserCache();
        E.checkArgument(this.users.exists(belong.source()),
                        "Not exists user '%s'", belong.source());
        E.checkArgument(this.groups.exists(belong.target()),
                        "Not exists group '%s'", belong.target());
        return this.belong.add(belong);
    }

    @Override
    public Id updateBelong(VortexBelong belong) {
        this.invalidateUserCache();
        return this.belong.update(belong);
    }

    @Override
    public VortexBelong deleteBelong(Id id) {
        this.invalidateUserCache();
        return this.belong.delete(id);
    }

    @Override
    public VortexBelong getBelong(Id id) {
        return this.belong.get(id);
    }

    @Override
    public List<VortexBelong> listBelong(List<Id> ids) {
        return this.belong.list(ids);
    }

    @Override
    public List<VortexBelong> listAllBelong(long limit) {
        return this.belong.list(limit);
    }

    @Override
    public List<VortexBelong> listBelongByUser(Id user, long limit) {
        return this.belong.list(user, Directions.OUT,
                                VortexBelong.P.BELONG, limit);
    }

    @Override
    public List<VortexBelong> listBelongByGroup(Id group, long limit) {
        return this.belong.list(group, Directions.IN,
                                VortexBelong.P.BELONG, limit);
    }

    @Override
    public Id createAccess(VortexAccess access) {
        this.invalidateUserCache();
        E.checkArgument(this.groups.exists(access.source()),
                        "Not exists group '%s'", access.source());
        E.checkArgument(this.targets.exists(access.target()),
                        "Not exists target '%s'", access.target());
        return this.access.add(access);
    }

    @Override
    public Id updateAccess(VortexAccess access) {
        this.invalidateUserCache();
        return this.access.update(access);
    }

    @Override
    public VortexAccess deleteAccess(Id id) {
        this.invalidateUserCache();
        return this.access.delete(id);
    }

    @Override
    public VortexAccess getAccess(Id id) {
        return this.access.get(id);
    }

    @Override
    public List<VortexAccess> listAccess(List<Id> ids) {
        return this.access.list(ids);
    }

    @Override
    public List<VortexAccess> listAllAccess(long limit) {
        return this.access.list(limit);
    }

    @Override
    public List<VortexAccess> listAccessByGroup(Id group, long limit) {
        return this.access.list(group, Directions.OUT,
                                VortexAccess.P.ACCESS, limit);
    }

    @Override
    public List<VortexAccess> listAccessByTarget(Id target, long limit) {
        return this.access.list(target, Directions.IN,
                                VortexAccess.P.ACCESS, limit);
    }

    @Override
    public Id createProject(VortexProject project) {
        E.checkArgument(!StringUtils.isEmpty(project.name()),
                        "The name of project can't be null or empty");
        return commit(() -> {
            // Create project admin group
            if (project.adminGroupId() == null) {
                VortexGroup adminGroup = new VortexGroup("admin_" + project.name());
                /*
                 * "creator" is a necessary parameter, other places are passed
                 * in "AuthManagerProxy", but here is the underlying module, so
                 * pass it directly here
                 */
                adminGroup.creator(project.creator());
                Id adminGroupId = this.createGroup(adminGroup);
                project.adminGroupId(adminGroupId);
            }

            // Create project op group
            if (project.opGroupId() == null) {
                VortexGroup opGroup = new VortexGroup("op_" + project.name());
                // Ditto
                opGroup.creator(project.creator());
                Id opGroupId = this.createGroup(opGroup);
                project.opGroupId(opGroupId);
            }

            // Create project target to verify permission
            final String targetName = "project_res_" + project.name();
            VortexResource resource = new VortexResource(ResourceType.PROJECT,
                                                     project.name(),
                                                     null);
            VortexTarget target = new VortexTarget(targetName,
                                               this.graph.name(),
                                               "localhost:8080",
                                               ImmutableList.of(resource));
            // Ditto
            target.creator(project.creator());
            Id targetId = this.targets.add(target);
            project.targetId(targetId);

            Id adminGroupId = project.adminGroupId();
            Id opGroupId = project.opGroupId();
            VortexAccess adminGroupWriteAccess = new VortexAccess(
                                                   adminGroupId, targetId,
                                                   VortexPermission.WRITE);
            // Ditto
            adminGroupWriteAccess.creator(project.creator());
            VortexAccess adminGroupReadAccess = new VortexAccess(
                                                  adminGroupId, targetId,
                                                  VortexPermission.READ);
            // Ditto
            adminGroupReadAccess.creator(project.creator());
            VortexAccess opGroupReadAccess = new VortexAccess(opGroupId, targetId,
                                                          VortexPermission.READ);
            // Ditto
            opGroupReadAccess.creator(project.creator());
            this.access.add(adminGroupWriteAccess);
            this.access.add(adminGroupReadAccess);
            this.access.add(opGroupReadAccess);
            return this.project.add(project);
        });
    }

    @Override
    public VortexProject deleteProject(Id id) {
        return this.commit(() -> {
            LockUtil.Locks locks = new LockUtil.Locks(this.graph.name());
            try {
                locks.lockWrites(LockUtil.PROJECT_UPDATE, id);

                VortexProject oldProject = this.project.get(id);
                /*
                 * Check whether there are any graph binding this project,
                 * throw ForbiddenException, if it is
                 */
                if (!CollectionUtils.isEmpty(oldProject.graphs())) {
                    String errInfo = String.format("Can't delete project '%s' " +
                                                   "that contains any graph, " +
                                                   "there are graphs bound " +
                                                   "to it", id);
                    throw new ForbiddenException(errInfo);
                }
                VortexProject project = this.project.delete(id);
                E.checkArgumentNotNull(project,
                                       "Failed to delete the project '%s'",
                                       id);
                E.checkArgumentNotNull(project.adminGroupId(),
                                       "Failed to delete the project '%s'," +
                                       "the admin group of project can't " +
                                       "be null", id);
                E.checkArgumentNotNull(project.opGroupId(),
                                       "Failed to delete the project '%s'," +
                                       "the op group of project can't be null",
                                       id);
                E.checkArgumentNotNull(project.targetId(),
                                       "Failed to delete the project '%s', " +
                                       "the target resource of project " +
                                       "can't be null", id);
                // Delete admin group
                this.groups.delete(project.adminGroupId());
                // Delete op group
                this.groups.delete(project.opGroupId());
                // Delete project_target
                this.targets.delete(project.targetId());
                return project;
            } finally {
                locks.unlock();
            }
        });
    }

    @Override
    public Id updateProject(VortexProject project) {
        return this.project.update(project);
    }

    @Override
    public Id projectAddGraphs(Id id, Set<String> graphs) {
        E.checkArgument(!CollectionUtils.isEmpty(graphs),
                        "Failed to add graphs to project '%s', the graphs " +
                        "parameter can't be empty", id);

        LockUtil.Locks locks = new LockUtil.Locks(this.graph.name());
        try {
            locks.lockWrites(LockUtil.PROJECT_UPDATE, id);

            VortexProject project = this.project.get(id);
            Set<String> sourceGraphs = new HashSet<>(project.graphs());
            int oldSize = sourceGraphs.size();
            sourceGraphs.addAll(graphs);
            // Return if there is none graph been added
            if (sourceGraphs.size() == oldSize) {
                return id;
            }
            project.graphs(sourceGraphs);
            return this.project.update(project);
        } finally {
            locks.unlock();
        }
    }

    @Override
    public Id projectRemoveGraphs(Id id, Set<String> graphs) {
        E.checkArgumentNotNull(id,
                               "Failed to remove graphs, the project id " +
                               "parameter can't be null");
        E.checkArgument(!CollectionUtils.isEmpty(graphs),
                        "Failed to delete graphs from the project '%s', " +
                        "the graphs parameter can't be null or empty", id);

        LockUtil.Locks locks = new LockUtil.Locks(this.graph.name());
        try {
            locks.lockWrites(LockUtil.PROJECT_UPDATE, id);

            VortexProject project = this.project.get(id);
            Set<String> sourceGraphs = new HashSet<>(project.graphs());
            int oldSize = sourceGraphs.size();
            sourceGraphs.removeAll(graphs);
            // Return if there is none graph been removed
            if (sourceGraphs.size() == oldSize) {
                return id;
            }
            project.graphs(sourceGraphs);
            return this.project.update(project);
        } finally {
            locks.unlock();
        }
    }

    @Override
    public VortexProject getProject(Id id) {
        return this.project.get(id);
    }

    @Override
    public List<VortexProject> listAllProject(long limit) {
        return this.project.list(limit);
    }

    @Override
    public VortexUser matchUser(String name, String password) {
        E.checkArgumentNotNull(name, "User name can't be null");
        E.checkArgumentNotNull(password, "User password can't be null");

        VortexUser user = this.findUser(name);
        if (user == null) {
            return null;
        }

        if (password.equals(this.pwdCache.get(user.id()))) {
            return user;
        }

        if (StringEncoding.checkPassword(password, user.password())) {
            this.pwdCache.update(user.id(), password);
            return user;
        }
        return null;
    }

    @Override
    public RolePermission rolePermission(AuthElement element) {
        if (element instanceof VortexUser) {
            return this.rolePermission((VortexUser) element);
        } else if (element instanceof VortexTarget) {
            return this.rolePermission((VortexTarget) element);
        }

        List<VortexAccess> accesses = new ArrayList<>();
        if (element instanceof VortexBelong) {
            VortexBelong belong = (VortexBelong) element;
            accesses.addAll(this.listAccessByGroup(belong.target(), -1));
        } else if (element instanceof VortexGroup) {
            VortexGroup group = (VortexGroup) element;
            accesses.addAll(this.listAccessByGroup(group.id(), -1));
        } else if (element instanceof VortexAccess) {
            VortexAccess access = (VortexAccess) element;
            accesses.add(access);
        } else {
            E.checkArgument(false, "Invalid type for role permission: %s",
                            element);
        }

        return this.rolePermission(accesses);
    }

    private RolePermission rolePermission(VortexUser user) {
        if (user.role() != null) {
            // Return cached role (40ms => 10ms)
            return user.role();
        }

        // Collect accesses by user
        List<VortexAccess> accesses = new ArrayList<>();
        List<VortexBelong> belongs = this.listBelongByUser(user.id(), -1);
        for (VortexBelong belong : belongs) {
            accesses.addAll(this.listAccessByGroup(belong.target(), -1));
        }

        // Collect permissions by accesses
        RolePermission role = this.rolePermission(accesses);

        user.role(role);
        return role;
    }

    private RolePermission rolePermission(List<VortexAccess> accesses) {
        // Mapping of: graph -> action -> resource
        RolePermission role = new RolePermission();
        for (VortexAccess access : accesses) {
            VortexPermission accessPerm = access.permission();
            VortexTarget target = this.getTarget(access.target());
            role.add(target.graph(), accessPerm, target.resources());
        }
        return role;
    }

    private RolePermission rolePermission(VortexTarget target) {
        RolePermission role = new RolePermission();
        // TODO: improve for the actual meaning
        role.add(target.graph(), VortexPermission.READ, target.resources());
        return role;
    }

    @Override
    public String loginUser(String username, String password)
                            throws AuthenticationException {
        VortexUser user = this.matchUser(username, password);
        if (user == null) {
            String msg = "Incorrect username or password";
            throw new AuthenticationException(msg);
        }

        Map<String, ?> payload = ImmutableMap.of(AuthConstant.TOKEN_USER_NAME,
                                                 username,
                                                 AuthConstant.TOKEN_USER_ID,
                                                 user.id.asString());
        String token = this.tokenGenerator.create(payload, this.tokenExpire);

        this.tokenCache.update(IdGenerator.of(token), username);
        return token;
    }

    @Override
    public void logoutUser(String token) {
        this.tokenCache.invalidate(IdGenerator.of(token));
    }

    @Override
    public UserWithRole validateUser(String username, String password) {
        VortexUser user = this.matchUser(username, password);
        if (user == null) {
            return new UserWithRole(username);
        }
        return new UserWithRole(user.id, username, this.rolePermission(user));
    }

    @Override
    public UserWithRole validateUser(String token) {
        String username = this.tokenCache.get(IdGenerator.of(token));

        Claims payload = null;
        boolean needBuildCache = false;
        if (username == null) {
            payload = this.tokenGenerator.verify(token);
            username = (String) payload.get(AuthConstant.TOKEN_USER_NAME);
            needBuildCache = true;
        }

        VortexUser user = this.findUser(username);
        if (user == null) {
            return new UserWithRole(username);
        } else if (needBuildCache) {
            long expireAt = payload.getExpiration().getTime();
            long bornTime = this.tokenCache.expire() -
                            (expireAt - System.currentTimeMillis());
            this.tokenCache.update(IdGenerator.of(token), username,
                                   Math.negateExact(bornTime));
        }

        return new UserWithRole(user.id(), username, this.rolePermission(user));
    }

    /**
     * Maybe can define an proxy class to choose forward or call local
     */
    public static boolean isLocal(AuthManager authManager) {
        return authManager instanceof StandardAuthManager;
    }

    public <R> R commit(Callable<R> callable) {
        this.groups.autoCommit(false);
        this.access.autoCommit(false);
        this.targets.autoCommit(false);
        this.project.autoCommit(false);
        this.belong.autoCommit(false);
        this.users.autoCommit(false);

        try {
            R result = callable.call();
            this.graph.systemTransaction().commit();
            return result;
        } catch (Throwable e) {
            this.groups.autoCommit(true);
            this.access.autoCommit(true);
            this.targets.autoCommit(true);
            this.project.autoCommit(true);
            this.belong.autoCommit(true);
            this.users.autoCommit(true);
            try {
                this.graph.systemTransaction().rollback();
            } catch (Throwable rollbackException) {
                LOG.error("Failed to rollback transaction: {}",
                          rollbackException.getMessage(), rollbackException);
            }
            if (e instanceof VortexException) {
                throw (VortexException) e;
            } else {
                throw new VortexException("Failed to commit transaction: %s",
                                        e.getMessage(), e);
            }
        }
    }
}
