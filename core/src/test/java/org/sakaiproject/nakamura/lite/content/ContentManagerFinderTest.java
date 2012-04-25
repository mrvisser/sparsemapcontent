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
package org.sakaiproject.nakamura.lite.content;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.infinispan.io.GridFilesystem;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.sakaiproject.nakamura.api.lite.ClientPoolException;
import org.sakaiproject.nakamura.api.lite.Configuration;
import org.sakaiproject.nakamura.api.lite.IndexDocument;
import org.sakaiproject.nakamura.api.lite.IndexDocumentFactory;
import org.sakaiproject.nakamura.api.lite.StorageClientException;
import org.sakaiproject.nakamura.api.lite.accesscontrol.AccessDeniedException;
import org.sakaiproject.nakamura.api.lite.accesscontrol.PrincipalValidatorResolver;
import org.sakaiproject.nakamura.api.lite.authorizable.User;
import org.sakaiproject.nakamura.api.lite.content.Content;
import org.sakaiproject.nakamura.lite.BaseMemoryRepository;
import org.sakaiproject.nakamura.lite.LoggingStorageListener;
import org.sakaiproject.nakamura.lite.RepositoryImpl;
import org.sakaiproject.nakamura.lite.accesscontrol.AccessControlManagerImpl;
import org.sakaiproject.nakamura.lite.accesscontrol.AuthenticatorImpl;
import org.sakaiproject.nakamura.lite.accesscontrol.PrincipalValidatorResolverImpl;
import org.sakaiproject.nakamura.lite.authorizable.AuthorizableActivator;
import org.sakaiproject.nakamura.lite.storage.spi.StorageClient;
import org.sakaiproject.nakamura.lite.storage.spi.StorageClientPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public class ContentManagerFinderTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractContentManagerTest.class);
  private RepositoryImpl repository;
  private GridFilesystem fs;
  private StorageClient client;
  private Configuration configuration;
  private StorageClientPool clientPool;
  private PrincipalValidatorResolver principalValidatorResolver = new PrincipalValidatorResolverImpl();

  @Before
  public void before() throws StorageClientException, AccessDeniedException, ClientPoolException,
          ClassNotFoundException, IOException {
    
      // create a repository and bind the test document factory to it.
      repository = (new BaseMemoryRepository()).getRepository();
      repository.bindIndexDocumentFactory(new TestIndexDocumentFactory());
      
      configuration = repository.getConfiguration();
      clientPool = repository.getConnectionPool();
      client = clientPool.getClient();
      fs = repository.getGridFilesystem();
      AuthorizableActivator authorizableActivator = new AuthorizableActivator(client,
              configuration);
      authorizableActivator.setup();
      LOGGER.info("Setup Complete");
  }

  @After
  public void after() throws ClientPoolException {
      repository.deactivate(null);
  }
  
  @Test
  public void testSimpleFind() throws StorageClientException, AccessDeniedException {
      AuthenticatorImpl AuthenticatorImpl = new AuthenticatorImpl(client, configuration, null);
      User currentUser = AuthenticatorImpl.authenticate("admin", "admin");

      AccessControlManagerImpl accessControlManager = new AccessControlManagerImpl(client,
              currentUser, configuration, null, new LoggingStorageListener(),
              principalValidatorResolver);

      ContentManagerImpl contentManager = new ContentManagerImpl(fs, client, accessControlManager,
              configuration, null, new LoggingStorageListener());
      contentManager.update(new Content("/simpleFind", ImmutableMap.of("sakai:marker",
              (Object) "testSimpleFindvalue1")));
      contentManager.update(new Content("/simpleFind/item2", ImmutableMap.of("sakai:marker",
              (Object) "testSimpleFindvalue1")));
      contentManager.update(new Content("/simpleFind/test", ImmutableMap.of("sakai:marker",
              (Object) "testSimpleFindvalue3")));
      contentManager.update(new Content("/simpleFind/test/ing", ImmutableMap.of("sakai:marker",
              (Object) "testSimpleFindvalue4")));

      verifyResults(contentManager.find(createSimpleMarkerQuery("testSimpleFindvalue4")),
              ImmutableSet.of("/simpleFind/test/ing"));
      verifyResults(contentManager.find(createSimpleMarkerQuery("testSimpleFindvalue1")),
              ImmutableSet.of("/simpleFind", "/simpleFind/item2"));
  }

  protected void verifyResults(Iterable<Content> ic, Set<String> shouldFind) {
      int i = 0;
      for (Content c : ic) {
          String path = c.getPath();
          if (shouldFind.contains(c.getPath())) {
              i++;
          } else {
              LOGGER.info("Found wrong content {}", path);
          }
      }
      Assert.assertEquals(shouldFind.size(), i);
  }

  private Query createSimpleMarkerQuery(String markerValue) {
    return new TermQuery(new Term("marker", markerValue));
  }
  
  /**
   * Index document factory used to test searches in this test suite.
   */
  public class TestIndexDocumentFactory implements IndexDocumentFactory {

    /**
     * {@inheritDoc}
     * @see org.sakaiproject.nakamura.api.lite.IndexDocumentFactory#createIndexDocument(java.lang.String, java.util.Map)
     */
    public IndexDocument createIndexDocument(String path, Map<String, Object> properties) {
      String marker = (String) properties.get("sakai:marker");
      if (marker != null) {
        TestIndexDocument doc = new TestIndexDocument();
        doc.id = path;
        doc.marker = marker;
        return doc;
      }
      return null;
    }
  }
}
