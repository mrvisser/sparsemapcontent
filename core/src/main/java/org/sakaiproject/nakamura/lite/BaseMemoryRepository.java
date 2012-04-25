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
import com.google.common.collect.Maps;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.sakaiproject.nakamura.api.lite.ClientPoolException;
import org.sakaiproject.nakamura.api.lite.IndexDocumentFactory;
import org.sakaiproject.nakamura.api.lite.StorageClientException;
import org.sakaiproject.nakamura.api.lite.accesscontrol.AccessDeniedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 * Utility class to create an entirely in memory Sparse Repository, useful for
 * testing or bulk internal modifications.
 */
public class BaseMemoryRepository {

    private static final Logger LOGGER = LoggerFactory.getLogger(BaseMemoryRepository.class);
    private ConfigurationImpl configuration;
    private RepositoryImpl repository;

    public BaseMemoryRepository() throws StorageClientException, AccessDeniedException,
            ClientPoolException, ClassNotFoundException, IOException {
      
        configuration = new ConfigurationImpl();
        Map<String, Object> properties = Maps.newHashMap();
        properties.put("acl-column-family", "ac");
        properties.put("authorizable-column-family", "au");
        configuration.activate(properties);
        
        DefaultCacheManager cacheContainer = new DefaultCacheManager(true);
        cacheContainer.defineConfiguration(configuration.getContentBodyCacheName(),
            createInMemoryCache());
        cacheContainer.defineConfiguration(configuration.getContentMetadataName(),
            createInMemoryCache());
        cacheContainer.defineConfiguration(configuration.getAuthCacheName(),
            createInMemoryCache());
        cacheContainer.defineConfiguration(configuration.getIndexCacheName(),
            createInMemoryIndexedCache());
        
        repository = new RepositoryImpl(cacheContainer, configuration,
            new LoggingStorageListener(), Collections.<IndexDocumentFactory>emptyList());
    }

    public void close() {
      repository.deactivate(ImmutableMap.<String, Object>of());
    }

    public RepositoryImpl getRepository() {
        return repository;
    }

    private org.infinispan.configuration.cache.Configuration createInMemoryCache() {
      return (new ConfigurationBuilder()).build();
    }
    
    private org.infinispan.configuration.cache.Configuration createInMemoryIndexedCache() {
      return (new ConfigurationBuilder()).indexing().enable().indexLocalOnly(true)
          .addProperty("hibernate.search.oae.directory_provider", "ram")
          .addProperty("hibernate.search.lucene_version", "LUCENE_35").build();
    }
}
