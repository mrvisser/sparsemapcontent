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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.commons.lang.StringUtils;
import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.ReferenceCardinality;
import org.apache.felix.scr.annotations.ReferencePolicy;
import org.apache.felix.scr.annotations.Service;
import org.infinispan.Cache;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.Parser;
import org.infinispan.io.GridFile.Metadata;
import org.infinispan.io.GridFilesystem;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.DefaultCacheManager;
import org.osgi.service.component.ComponentContext;
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
import org.sakaiproject.nakamura.lite.authorizable.AuthorizableIndexDocumentFactory;
import org.sakaiproject.nakamura.lite.jndi.DummyJndiContextFactory;
import org.sakaiproject.nakamura.lite.storage.infinispan.InfinispanStorageClientPool;
import org.sakaiproject.nakamura.lite.storage.spi.StorageClient;
import org.sakaiproject.nakamura.lite.storage.spi.StorageClientPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collection;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

@Component(immediate = true, metatype = true)
@Service(value = Repository.class)
public class RepositoryImpl implements Repository {
  
    private static final Logger LOGGER = LoggerFactory.getLogger(RepositoryImpl.class);
    
    private static final Collection<IndexDocumentFactory> INDEXED_DOCUMENTS_INTERNAL =
        ImmutableSet.<IndexDocumentFactory>of(new AuthorizableIndexDocumentFactory());

    private static final String JNDI_CACHE_NAME = "sakai/oae/cacheContainer";
    private static final Hashtable<String, String> JNDI_ENV = new Hashtable<String, String>(
        ImmutableMap.<String, String>of(Context.INITIAL_CONTEXT_FACTORY, DummyJndiContextFactory.class.getName()));
    
    @Property(label="Infinispan Configuration", description="A URL that points to the Infinispan configuration. " +
    		"If left empty, an internal default configuration (not recommended for production!) will be used.")
    public static final String CFG_CONFIG_FILE_URL = "org.sakaiproject.nakamura.lite.RespositoryImpl.config_url";
    
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

    protected StorageClientPool clientPool;
    
    protected CacheContainer cacheContainer;
    
    protected GridFilesystem fs;
    
    public RepositoryImpl() {
    }

    public RepositoryImpl(CacheContainer cacheContainer, Configuration configuration,
        LoggingStorageListener listener, List<IndexDocumentFactory> indexes)
            throws StorageClientException, AccessDeniedException {
      this.configuration = configuration;
      this.storeListener = listener;
      this.cacheContainer = cacheContainer;
      this.indexes.addAll(indexes);
      doStandardActivation();
    }

    @Activate
    public void activate(final ComponentContext componentContext) throws ClientPoolException,
        StorageClientException, AccessDeniedException, IOException, ClassNotFoundException,
        NamingException {
      
      // I think this is safe enough:
      // http://svn.apache.org/repos/asf/felix/trunk/scr/src/main/java/org/apache/felix/scr/impl/helper/ActivateMethod.java
      @SuppressWarnings("unchecked")
      Map<String, String> properties = (Map<String, String>) componentContext.getProperties();
      
      ClassLoader classLoader = getClass().getClassLoader();
      
      /*
      // grab the bundle classloader *sigh*
      classLoader = componentContext.getBundleContext().getBundle()
          .loadClass("org.infinispan.factories.GlobalComponentRegistry").getClassLoader();
       */
      InputStream configStream = resolveConfiguration(classLoader, (String) properties.get(CFG_CONFIG_FILE_URL));
      ConfigurationBuilderHolder config = new Parser(classLoader).parse(configStream);
      
      // set the custom classloader to the bundle classloader
      config.getGlobalConfigurationBuilder().classLoader(classLoader);
      
      // force the index cache to be indexed, and wire together the jndi stuff so
      // deployers don't have to worry about this.
      config.getNamedConfigurationBuilders().get(configuration.getIndexCacheName())
          .indexing().enabled(true)
          .addProperty("hibernate.search.infinispan.cachemanager_jndiname", JNDI_CACHE_NAME)
          .addProperty("hibernate.jndi.class", JNDI_ENV.get(Context.INITIAL_CONTEXT_FACTORY));

      cacheContainer = new DefaultCacheManager(config, true);

      ClassLoader prev = ClassLoaderHelper.swapContext(getClass().getClassLoader());
      try {
        // stuff the cache container into the jndi context where it can be accessed by
        // the hibernate cachemanager lookup
        InitialContext ctx = new InitialContext(JNDI_ENV);
        ctx.bind(JNDI_CACHE_NAME, cacheContainer);
        ctx.close();
      
        doStandardActivation();
      } finally {
        ClassLoaderHelper.swapContext(prev);
      }

    }

    private void doStandardActivation() throws StorageClientException, AccessDeniedException {
      StorageClient client = null;
      try {
        indexes.addAll(INDEXED_DOCUMENTS_INTERNAL);
        
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
    public void deactivate(Map<String, Object> properties) {
      ClassLoader prev = ClassLoaderHelper.swapContext(getClass().getClassLoader());
      try {
        InitialContext ctx = new InitialContext(JNDI_ENV);
        ctx.unbind(JNDI_CACHE_NAME);
        ctx.close();
      } catch (NamingException e) {
        LOGGER.warn("Error closing JNDI context. This is probably harmless as the " +
        		"dummy context does not hold any resources.", e);
      } finally {
        ClassLoaderHelper.swapContext(prev);
      }
      
      indexes.clear();
      if (cacheContainer != null) {
        cacheContainer.stop();
      }
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
    
    private InputStream resolveConfiguration(ClassLoader cl, String urlStr) throws IOException {
      InputStream configStream = null;
      if (!StringUtils.isBlank(urlStr)) {
        try {
          URL url = new URL(urlStr);
          configStream = url.openStream();
        } catch (MalformedURLException e) {
          LOGGER.warn("Could not open infinispan configuration URL: {}. Falling back to internal configuration.",
              urlStr);
        } catch (IOException e) {
          LOGGER.warn("Could not open infinispan configuration URL: {}. Falling back to internal configuration.",
              urlStr);
        }
      }
      
      // fall back to internal default
      if (configStream == null) {
        try {
          configStream = getInternalConfiguration(cl);
        } catch (IOException e) {
          LOGGER.error("Could not open internal infinispan configuration", e);
          throw e;
        }
      }
      
      return configStream;
    }

    private InputStream getInternalConfiguration(ClassLoader cl) throws IOException {
      URL url = cl.getResource("org/sakaiproject/nakamura/lite/cfg/infinispan-default.xml");
      return url.openStream();
    }
    
    public void setConfiguration(Configuration configuration) {
        this.configuration = configuration;
    }

    public Configuration getConfiguration() {
      return this.configuration;
    }
    
    public StorageClientPool getConnectionPool() {
      return this.clientPool;
    }
    
    public CacheContainer getCacheContainer() {
      return this.cacheContainer;
    }
    
    public GridFilesystem getGridFilesystem() {
      return this.fs;
    }
    
    public void setConnectionPool(StorageClientPool connectionPool) {
        this.clientPool = connectionPool;
    }

    public void setStorageListener(StoreListener storeListener) {
        this.storeListener = storeListener;

    }
    
    public void bindIndexDocumentFactory(IndexDocumentFactory factory) throws ClientPoolException {
      indexes.add(factory);
    }

    public void unbindIndexDocumentFactory(IndexDocumentFactory factory)
        throws ClientPoolException {
      indexes.remove(factory);
    }

}
