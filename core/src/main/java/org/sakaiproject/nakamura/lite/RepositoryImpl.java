/*
 * Licensed to the Sakai Foundation (SF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The SF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.sakaiproject.nakamura.lite;

import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.ReferenceCardinality;
import org.apache.felix.scr.annotations.ReferencePolicy;
import org.apache.felix.scr.annotations.Service;
import org.infinispan.Cache;
import org.infinispan.io.GridFile.Metadata;
import org.infinispan.io.GridFilesystem;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.DefaultCacheManager;
import org.sakaiproject.nakamura.api.lite.CacheHolder;
import org.sakaiproject.nakamura.api.lite.ClientPoolException;
import org.sakaiproject.nakamura.api.lite.Configuration;
import org.sakaiproject.nakamura.api.lite.IndexDocumentFactory;
import org.sakaiproject.nakamura.api.lite.Repository;
import org.sakaiproject.nakamura.api.lite.Session;
import org.sakaiproject.nakamura.api.lite.StorageCacheManager;
import org.sakaiproject.nakamura.api.lite.StorageClientException;
import org.sakaiproject.nakamura.api.lite.StoreListener;
import org.sakaiproject.nakamura.api.lite.accesscontrol.AccessDeniedException;
import org.sakaiproject.nakamura.api.lite.accesscontrol.PrincipalValidatorResolver;
import org.sakaiproject.nakamura.api.lite.authorizable.User;
import org.sakaiproject.nakamura.lite.accesscontrol.AuthenticatorImpl;
import org.sakaiproject.nakamura.lite.authorizable.AuthorizableActivator;
import org.sakaiproject.nakamura.lite.storage.infinispan.InfinispanStorageClientPool;
import org.sakaiproject.nakamura.lite.storage.spi.StorageClient;
import org.sakaiproject.nakamura.lite.storage.spi.StorageClientPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

@Component(immediate = true, metatype = true)
@Service(value = Repository.class)
public class RepositoryImpl implements Repository {
  
    private static final Logger LOGGER = LoggerFactory.getLogger(RepositoryImpl.class);

    @Reference
    protected Configuration configuration;

    /**
     * A list of index document factories that describes, what content should be indexed and how to
     * index it.
     */
    @Reference(cardinality = ReferenceCardinality.OPTIONAL_MULTIPLE,
        policy = ReferencePolicy.DYNAMIC, referenceInterface = IndexDocumentFactory.class,
        bind = "bindIndexDocumentFactory", unbind = "unbindIndexDocumentFactory")
    protected CopyOnWriteArrayList<IndexDocumentFactory> indexes =
        new CopyOnWriteArrayList<IndexDocumentFactory>();
    
    @Reference
    protected StoreListener storeListener;

    @Reference
    protected PrincipalValidatorResolver principalValidatorResolver;

    @Reference
    protected StorageClientPool clientPool;
    
    protected CacheContainer cacheContainer;
    
    protected GridFilesystem fs;
    
    public RepositoryImpl() {
    }

    public RepositoryImpl(CacheContainer cacheContainer, Configuration configuration,
        LoggingStorageListener listener) throws StorageClientException, AccessDeniedException {
      this.configuration = configuration;
      this.storeListener = listener;
      this.cacheContainer = cacheContainer;
      doStandardActivation();
    }

    @Activate
    public void activate(Map<String, Object> properties) throws ClientPoolException,
            StorageClientException, AccessDeniedException {
          // set up the caches
          cacheContainer = new DefaultCacheManager(true);
          //TODO: configure the cache container
          doStandardActivation();
    }

    private void doStandardActivation() throws StorageClientException, AccessDeniedException {
      StorageClient client = null;
      try {
        clientPool = new InfinispanStorageClientPool(cacheContainer, configuration, indexes);
        client = clientPool.getClient();
          
        // setup the authorizables
        AuthorizableActivator authorizableActivator = new AuthorizableActivator(client,
            configuration);
        authorizableActivator.setup();
        
        // set up the content store
        Cache<String, byte[]> contentBodyCache = cacheContainer.getCache(
            configuration.getContentBodyCacheName());
        Cache<String, Metadata> contentMetadataCache = cacheContainer.getCache(
            configuration.getContentMetadataName());
        fs = new GridFilesystem(contentBodyCache, contentMetadataCache);
      } finally {
        if (client != null) {
          client.close();
        } else {
          LOGGER.error("Failed to actvate repository, probably failed to create default users");
        }
      }
    }
    
    @Deactivate
    public void deactivate(Map<String, Object> properties) throws ClientPoolException {
      if (cacheContainer != null)
        cacheContainer.stop();
    }

    public Session login(String username, String password) throws ClientPoolException,
            StorageClientException, AccessDeniedException {
        return openSession(username, password);
    }

    public Session login() throws ClientPoolException, StorageClientException,
            AccessDeniedException {
        return openSession(User.ANON_USER);
    }

    public Session loginAdministrative() throws ClientPoolException, StorageClientException,
            AccessDeniedException {
        return openSession(User.ADMIN_USER);
    }

    public Session loginAdministrative(String username) throws StorageClientException,
            ClientPoolException, AccessDeniedException {
        return openSession(username);
    }

    public Session loginAdministrativeBypassEnable(String username) throws StorageClientException,
            ClientPoolException, AccessDeniedException {
        return openSessionBypassEnable(username);
    }

    private Session openSession(String username, String password) throws StorageClientException,
            AccessDeniedException {
        StorageClient client = null;
        try {
            client = clientPool.getClient();
            AuthenticatorImpl authenticatorImpl = new AuthenticatorImpl(client, configuration, getAuthorizableCache(clientPool.getStorageCacheManager()));
            User currentUser = authenticatorImpl.authenticate(username, password);
            if (currentUser == null) {
                throw new StorageClientException("User " + username + " cant login with password");
            }
            return new SessionImpl(this, currentUser, client, fs, configuration, storeListener,
                principalValidatorResolver);
        } catch (ClientPoolException e) {
            clientPool.getClient();
            throw e;
        } catch (StorageClientException e) {
            clientPool.getClient();
            throw e;
        } catch (AccessDeniedException e) {
            clientPool.getClient();
            throw e;
        } catch (Throwable e) {
            clientPool.getClient();
            throw new StorageClientException(e.getMessage(), e);
        }
    }

    private Map<String, CacheHolder> getAuthorizableCache(StorageCacheManager storageCacheManager) {
        if ( storageCacheManager != null ) {
            return storageCacheManager.getAuthorizableCache();
        }
        return null;
    }

    private Session openSession(String username) throws StorageClientException,
            AccessDeniedException {
        StorageClient client = null;
        try {
            client = clientPool.getClient();
            AuthenticatorImpl authenticatorImpl = new AuthenticatorImpl(client, configuration, getAuthorizableCache(clientPool.getStorageCacheManager()));
            User currentUser = authenticatorImpl.systemAuthenticate(username);
            if (currentUser == null) {
                throw new StorageClientException("User " + username
                        + " does not exist, cant login administratively as this user");
            }
            return new SessionImpl(this, currentUser, client, fs, configuration, storeListener,
                principalValidatorResolver);
        } catch (ClientPoolException e) {
            clientPool.getClient();
            throw e;
        } catch (StorageClientException e) {
            clientPool.getClient();
            throw e;
        } catch (AccessDeniedException e) {
            clientPool.getClient();
            throw e;
        } catch (Throwable e) {
            clientPool.getClient();
            throw new StorageClientException(e.getMessage(), e);
        }
    }

    private Session openSessionBypassEnable(String username) throws StorageClientException,
            AccessDeniedException {
        StorageClient client = null;
        try {
            client = clientPool.getClient();
            AuthenticatorImpl authenticatorImpl = new AuthenticatorImpl(client, configuration, getAuthorizableCache(clientPool.getStorageCacheManager()));
            User currentUser = authenticatorImpl.systemAuthenticateBypassEnable(username);
            if (currentUser == null) {
                throw new StorageClientException("User " + username
                        + " does not exist, cant login administratively as this user");
            }
            return new SessionImpl(this, currentUser, client, fs, configuration, storeListener,
                principalValidatorResolver);
        } catch (ClientPoolException e) {
            clientPool.getClient();
            throw e;
        } catch (StorageClientException e) {
            clientPool.getClient();
            throw e;
        } catch (AccessDeniedException e) {
            clientPool.getClient();
            throw e;
        } catch (Throwable e) {
            clientPool.getClient();
            throw new StorageClientException(e.getMessage(), e);
        }
    }

    public void setConfiguration(Configuration configuration) {
        this.configuration = configuration;
    }

    public void setConnectionPool(StorageClientPool connectionPool) {
        this.clientPool = connectionPool;
    }

    public void setStorageListener(StoreListener storeListener) {
        this.storeListener = storeListener;

    }
    
    protected void bindIndexDocumentFactory(IndexDocumentFactory factory) {
      indexes.add(factory);
    }

    protected void unbindIndexDocumentFactory(IndexDocumentFactory factory) {
      indexes.remove(factory);
    }

}
