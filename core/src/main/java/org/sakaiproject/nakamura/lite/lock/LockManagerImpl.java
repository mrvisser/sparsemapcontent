package org.sakaiproject.nakamura.lite.lock;

import java.util.Map;

import org.sakaiproject.nakamura.api.lite.CacheHolder;
import org.sakaiproject.nakamura.api.lite.Configuration;
import org.sakaiproject.nakamura.api.lite.StorageClientException;
import org.sakaiproject.nakamura.api.lite.StorageClientUtils;
import org.sakaiproject.nakamura.api.lite.authorizable.User;
import org.sakaiproject.nakamura.api.lite.lock.AlreadyLockedException;
import org.sakaiproject.nakamura.api.lite.lock.LockManager;
import org.sakaiproject.nakamura.api.lite.lock.LockState;
import org.sakaiproject.nakamura.lite.CachingManagerImpl;
import org.sakaiproject.nakamura.lite.storage.spi.StorageClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LockManagerImpl extends CachingManagerImpl implements LockManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(LockManagerImpl.class);

    public LockManagerImpl(StorageClient storageClient, Configuration config, User currentUser, Map<String, CacheHolder> sharedCache) {
        super(storageClient, sharedCache);
    }

    public void close() {
    }

    public String lock(String path, long expires, String extra) throws StorageClientException, AlreadyLockedException {
      return null;
    }
    
    public String refreshLock(String path, long timeoutInSeconds, String extra, String token) throws StorageClientException {
      return null;
    }
    

    public void unlock(String path, String token) throws StorageClientException {
    }
    
    public LockState getLockState(String path, String token) throws StorageClientException {
      return null;
    }
    

    public boolean isLocked(String path) throws StorageClientException {
      return false;
    }

    @Override
    protected Logger getLogger() {
        return LOGGER;
    }

}
