

package com.vortex.vortexdb.auth;

import com.vortex.vortexdb.auth.SchemaDefine.AuthElement;
import com.vortex.vortexdb.backend.id.Id;

import javax.security.sasl.AuthenticationException;
import java.util.List;
import java.util.Set;

public interface AuthManager {

    public boolean close();

    public Id createUser(VortexUser user);
    public Id updateUser(VortexUser user);
    public VortexUser deleteUser(Id id);
    public VortexUser findUser(String name);
    public VortexUser getUser(Id id);
    public List<VortexUser> listUsers(List<Id> ids);
    public List<VortexUser> listAllUsers(long limit);

    public Id createGroup(VortexGroup group);
    public Id updateGroup(VortexGroup group);
    public VortexGroup deleteGroup(Id id);
    public VortexGroup getGroup(Id id);
    public List<VortexGroup> listGroups(List<Id> ids);
    public List<VortexGroup> listAllGroups(long limit);

    public Id createTarget(VortexTarget target);
    public Id updateTarget(VortexTarget target);
    public VortexTarget deleteTarget(Id id);
    public VortexTarget getTarget(Id id);
    public List<VortexTarget> listTargets(List<Id> ids);
    public List<VortexTarget> listAllTargets(long limit);

    public Id createBelong(VortexBelong belong);
    public Id updateBelong(VortexBelong belong);
    public VortexBelong deleteBelong(Id id);
    public VortexBelong getBelong(Id id);
    public List<VortexBelong> listBelong(List<Id> ids);
    public List<VortexBelong> listAllBelong(long limit);
    public List<VortexBelong> listBelongByUser(Id user, long limit);
    public List<VortexBelong> listBelongByGroup(Id group, long limit);

    public Id createAccess(VortexAccess access);
    public Id updateAccess(VortexAccess access);
    public VortexAccess deleteAccess(Id id);
    public VortexAccess getAccess(Id id);
    public List<VortexAccess> listAccess(List<Id> ids);
    public List<VortexAccess> listAllAccess(long limit);
    public List<VortexAccess> listAccessByGroup(Id group, long limit);
    public List<VortexAccess> listAccessByTarget(Id target, long limit);

    public Id createProject(VortexProject project);
    public VortexProject deleteProject(Id id);
    public Id updateProject(VortexProject project);
    public Id projectAddGraphs(Id id, Set<String> graphs);
    public Id projectRemoveGraphs(Id id, Set<String> graphs);
    public VortexProject getProject(Id id);
    public List<VortexProject> listAllProject(long limit);

    public VortexUser matchUser(String name, String password);
    public RolePermission rolePermission(AuthElement element);

    public String loginUser(String username, String password)
                            throws AuthenticationException;
    public void logoutUser(String token);

    public UserWithRole validateUser(String username, String password);
    public UserWithRole validateUser(String token);
}
