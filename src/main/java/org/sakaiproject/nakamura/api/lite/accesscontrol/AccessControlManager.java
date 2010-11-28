package org.sakaiproject.nakamura.api.lite.accesscontrol;

import org.sakaiproject.nakamura.api.lite.StorageClientException;
import org.sakaiproject.nakamura.api.lite.authorizable.Authorizable;

import java.util.Map;

public interface AccessControlManager {

    Map<String, Object> getAcl(String objectType, String objectPath) throws StorageClientException,
            AccessDeniedException;

    void setAcl(String objectType, String objectPath, AclModification[] aclModifications)
            throws StorageClientException, AccessDeniedException;

    void check(String objectType, String objectPath, Permission permission)
            throws AccessDeniedException, StorageClientException;

    String getCurrentUserId();

    boolean can(Authorizable authorizable, String objectType, String objectPath,
            Permission permission);

}
