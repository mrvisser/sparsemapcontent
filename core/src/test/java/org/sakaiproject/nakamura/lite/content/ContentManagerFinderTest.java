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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
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
import org.sakaiproject.nakamura.api.lite.StorageClientUtils;
import org.sakaiproject.nakamura.api.lite.accesscontrol.AccessDeniedException;
import org.sakaiproject.nakamura.api.lite.accesscontrol.PrincipalValidatorResolver;
import org.sakaiproject.nakamura.api.lite.authorizable.User;
import org.sakaiproject.nakamura.api.lite.content.Content;
import org.sakaiproject.nakamura.api.lite.content.ContentManager;
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
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public class ContentManagerFinderTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(ContentManagerFinderTest.class);
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

      verifyResults(contentManager.find(createSimpleMarkerQuery("testSimpleFindvalue4"), null),
              ImmutableSet.of("/simpleFind/test/ing"));
      verifyResults(contentManager.find(createSimpleMarkerQuery("testSimpleFindvalue1"), null),
              ImmutableSet.of("/simpleFind", "/simpleFind/item2"));
  }
  
  @Test
  public void testSimpleFindWithSort() throws StorageClientException, AccessDeniedException {
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

      verifyResults(contentManager.find(createSimpleMarkerQuery("testSimpleFindvalue4"),
          createSimpleSort("marker")), ImmutableSet.of("/simpleFind/test/ing"));
      verifyResults(contentManager.find(createSimpleMarkerQuery("testSimpleFindvalue1"),
          createSimpleSort("marker")), ImmutableSet.of("/simpleFind", "/simpleFind/item2"));
  }

  
  @Test
  public void testSimpleArrayFind() throws StorageClientException, AccessDeniedException {
      AuthenticatorImpl AuthenticatorImpl = new AuthenticatorImpl(client, configuration, null);
      User currentUser = AuthenticatorImpl.authenticate("admin", "admin");

      AccessControlManagerImpl accessControlManager = new AccessControlManagerImpl(client,
              currentUser, configuration, null, new LoggingStorageListener(),
              principalValidatorResolver);

      ContentManagerImpl contentManager = new ContentManagerImpl(fs, client, accessControlManager,
          configuration, null, new LoggingStorageListener());
      contentManager.update(new Content("/simpleArrayFind", ImmutableMap.of("sakai:category",
              (Object) new String[] { "testSimpleArrayFindvalue88", "testSimpleArrayFindvalue1" })));
      contentManager.update(new Content("/simpleArrayFind/item2", ImmutableMap.of("sakai:category",
              (Object) new String[] { "testSimpleArrayFindvalue88", "testSimpleArrayFindvalue1" })));
      contentManager.update(new Content("/simpleArrayFind/test", ImmutableMap.of("sakai:category",
              (Object) new String[] { "testSimpleArrayFindvalue44", "testSimpleArrayFindvalue3" })));
      contentManager.update(new Content("/simpleArrayFind/test/ing", ImmutableMap.of(
              "sakai:category", (Object) new String[] { "testSimpleArrayFindvalue88", "testSimpleArrayFindvalue4" })));

      verifyResults(contentManager.find(createCategoryQuery("testSimpleArrayFindvalue4"), null),
              ImmutableSet.of("/simpleArrayFind/test/ing"));
      verifyResults(contentManager.find(createCategoryQuery("testSimpleArrayFindvalue1"), null),
              ImmutableSet.of("/simpleArrayFind", "/simpleArrayFind/item2"));
      verifyResults(contentManager.find(createCategoryQuery("testSimpleArrayFindvalue88"), null),
              ImmutableSet.of("/simpleArrayFind/test/ing", "/simpleArrayFind",
                      "/simpleArrayFind/item2"));
  }

  @Test
  public void testFindNoFilter() throws StorageClientException, AccessDeniedException {
      AuthenticatorImpl AuthenticatorImpl = new AuthenticatorImpl(client, configuration, null);
      User currentUser = AuthenticatorImpl.authenticate("admin", "admin");

      AccessControlManagerImpl accessControlManager = new AccessControlManagerImpl(client,
              currentUser, configuration, null, new LoggingStorageListener(),
              principalValidatorResolver);

      ContentManagerImpl contentManager = new ContentManagerImpl(fs, client, accessControlManager,
          configuration, null, new LoggingStorageListener());
      contentManager.update(new Content("/testFindNoFilter", ImmutableMap.of("sakai:category",
              (Object) new String[] { "testFindNoFiltervalue88", "testFindNoFiltervalue1" })));
      contentManager.update(new Content("/testFindNoFilter/item2", ImmutableMap.of("sakai:category",
              (Object) new String[] { "testFindNoFiltervalue88", "testFindNoFiltervalue1" })));
      contentManager.update(new Content("/testFindNoFilter/test", ImmutableMap.of("sakai:category",
              (Object) new String[] { "testFindNoFiltervalue44", "testFindNoFiltervalue3" })));
      contentManager.update(new Content("/testFindNoFilter/test/ing", ImmutableMap.of(
              "sakai:category", (Object) new String[] { "testFindNoFiltervalue88", "testSimpleArrayFindvalue4" })));

      Iterable<Content> found = contentManager.find(new TermQuery(new Term("nonExistentProperty",
          "testFindNoFiltervalue4")), null);
      Iterator<Content> foundIterator = found.iterator();
      Assert.assertFalse(foundIterator.hasNext());
  }
  
  /**
   * search for "a" find contentA
   * 
   * @throws StorageClientException
   * @throws AccessDeniedException
   */
  @Test
  public void testMultiValuedIndexSearchFindA() throws StorageClientException,
      AccessDeniedException {
    final ContentManager contentManager = setupMultiValuedIndexSearch();
    final Query searchCriteria = createQuery(MV.propField, MV.multiValueA[0]);
    final Iterable<Content> iterable = contentManager.find(searchCriteria, null);
    assertNotNull("Iterable should not be null", iterable);
    final Iterator<Content> iter = iterable.iterator();
    assertNotNull("Iterator should not be null", iter);
    assertTrue("Should have found a match", iter.hasNext());
    int found = 0;
    while (iter.hasNext()) {
      final Content match = iter.next();
      assertNotNull("match should not be null", match);
      assertEquals(MV.pathA, match.getPath());
      assertNotNull("match should have key: " + MV.propKey, match.getProperty(MV.propKey));
      assertTrue("String[] should be equal",
          Arrays.equals(MV.multiValueA, (String[]) match.getProperty(MV.propKey)));
      found++;
    }
    assertTrue("Should have found only one match; found: " + found, found == 1);
  }
  

  /**
   * search for "a" find contentA
   * 
   * @throws StorageClientException
   * @throws AccessDeniedException
   */
  @Test
  public void testMultiValuedIndexSearchFindA2() throws StorageClientException,
      AccessDeniedException {
    final ContentManager contentManager = setupMultiValuedIndexSearch();
    final Query searchCriteria = createQuery(MV.propField, MV.multiValueA[1]);
    final Iterable<Content> iterable = contentManager.find(searchCriteria, null);
    assertNotNull("Iterable should not be null", iterable);
    final Iterator<Content> iter = iterable.iterator();
    assertNotNull("Iterator should not be null", iter);
    assertTrue("Should have found a match", iter.hasNext());
    int found = 0;
    while (iter.hasNext()) {
      final Content match = iter.next();
      assertNotNull("match should not be null", match);
      assertEquals(MV.pathA, match.getPath());
      assertNotNull("match should have key: " + MV.propKey, match.getProperty(MV.propKey));
      assertTrue("String[] should be equal",
          Arrays.equals(MV.multiValueA, (String[]) match.getProperty(MV.propKey)));
      found++;
    }
    assertTrue("Should have found only one match; found: " + found, found == 1);
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
  
  /**
   * search for "x" find contentX only once
   * 
   * @throws StorageClientException
   * @throws AccessDeniedException
   */
  @Test
  public void testMultiValuedIndexSearchFindX() throws StorageClientException,
      AccessDeniedException {
    final ContentManager contentManager = setupMultiValuedIndexSearch();
    final Query searchCriteria = createQuery(MV.propField, MV.multiValueB[0]);
    final Iterable<Content> iterable = contentManager.find(searchCriteria, null);
    assertNotNull("Iterable should not be null", iterable);
    final Iterator<Content> iter = iterable.iterator();
    assertNotNull("Iterator should not be null", iter);
    assertTrue("Should have found a match", iter.hasNext());
    int found = 0;
    while (iter.hasNext()) {
      final Content match = iter.next();
      assertNotNull("match should not be null", match);
      assertEquals(MV.pathB, match.getPath());
      assertNotNull("match should have key: " + MV.propKey, match.getProperty(MV.propKey));
      assertTrue("String[] should be equal",
          Arrays.equals(MV.multiValueB, (String[]) match.getProperty(MV.propKey)));
      found++;
    }
    assertTrue("Should have found only one match; found: " + found, found == 1);
  }

  /**
   * search for "x" find contentX only once
   * 
   * @throws StorageClientException
   * @throws AccessDeniedException
   */
  @Test
  public void testMultiValuedIndexSearchFindX2() throws StorageClientException,
      AccessDeniedException {
    final ContentManager contentManager = setupMultiValuedIndexSearch();
    final Query searchCriteria = createQuery(MV.propField, MV.multiValueB[1]);
    final Iterable<Content> iterable = contentManager.find(searchCriteria, null);
    assertNotNull("Iterable should not be null", iterable);
    final Iterator<Content> iter = iterable.iterator();
    assertNotNull("Iterator should not be null", iter);
    assertTrue("Should have found a match", iter.hasNext());
    int found = 0;
    while (iter.hasNext()) {
      final Content match = iter.next();
      assertNotNull("match should not be null", match);
      assertEquals(MV.pathB, match.getPath());
      assertNotNull("match should have key: " + MV.propKey, match.getProperty(MV.propKey));
      assertTrue("String[] should be equal",
          Arrays.equals(MV.multiValueB, (String[]) match.getProperty(MV.propKey)));
      found++;
    }
    assertTrue("Should have found only one match; found: " + found, found == 1);
  }

  /**
   * search for "x" find contentX only once
   * 
   * @throws StorageClientException
   * @throws AccessDeniedException
   */
  @Test
  public void testMultiValuedIndexSearchFindX3() throws StorageClientException,
      AccessDeniedException {
    final ContentManager contentManager = setupMultiValuedIndexSearch();
    final Query searchCriteria = createQuery(MV.propField, MV.multiValueB[2]);
    final Iterable<Content> iterable = contentManager.find(searchCriteria, null);
    assertNotNull("Iterable should not be null", iterable);
    final Iterator<Content> iter = iterable.iterator();
    assertNotNull("Iterator should not be null", iter);
    assertTrue("Should have found a match", iter.hasNext());
    int found = 0;
    while (iter.hasNext()) {
      final Content match = iter.next();
      assertNotNull("match should not be null", match);
      assertEquals(MV.pathB, match.getPath());
      assertNotNull("match should have key: " + MV.propKey, match.getProperty(MV.propKey));
      assertTrue("String[] should be equal",
          Arrays.equals(MV.multiValueB, (String[]) match.getProperty(MV.propKey)));
      found++;
    }
    assertTrue("Should have found only one match; found: " + found, found == 1);
  }

  /**
   * search for "a" or "b" find contentA only once
   * 
   * @throws StorageClientException
   * @throws AccessDeniedException
   */
  @Test
  public void testMultiValuedIndexSearchFindAorB() throws StorageClientException,
      AccessDeniedException {
    final ContentManager contentManager = setupMultiValuedIndexSearch();
    final Query orSet = createOrQuery(MV.propField, MV.multiValueA);
    final Iterable<Content> iterable = contentManager.find(orSet, null);
    assertNotNull("Iterable should not be null", iterable);
    final Iterator<Content> iter = iterable.iterator();
    assertNotNull("Iterator should not be null", iter);
    assertTrue("Should have found a match", iter.hasNext());
    int found = 0;
    while (iter.hasNext()) {
      final Content match = iter.next();
      assertNotNull("match should not be null", match);
      assertEquals(MV.pathA, match.getPath());
      assertNotNull("match should have key: " + MV.propKey, match.getProperty(MV.propKey));
      assertTrue("String[] should be equal",
          Arrays.equals(MV.multiValueA, (String[]) match.getProperty(MV.propKey)));
      found++;
    }
    assertTrue("Should have found only one match; found: " + found, found == 1);
  }
  
  /**
   * search for "a" or "b" find contentA only once
   * 
   * @throws StorageClientException
   * @throws AccessDeniedException
   */
  @Test
  public void testMultiValuedIndexSearchFindBorA() throws StorageClientException,
      AccessDeniedException {
    final ContentManager contentManager = setupMultiValuedIndexSearch();
    final Query orSet = createOrQuery(MV.propField, new String[] { MV.multiValueA[1], MV.multiValueA[0] });
    final Iterable<Content> iterable = contentManager.find(orSet, null);
    assertNotNull("Iterable should not be null", iterable);
    final Iterator<Content> iter = iterable.iterator();
    assertNotNull("Iterator should not be null", iter);
    assertTrue("Should have found a match", iter.hasNext());
    int found = 0;
    while (iter.hasNext()) {
      final Content match = iter.next();
      assertNotNull("match should not be null", match);
      assertEquals(MV.pathA, match.getPath());
      assertNotNull("match should have key: " + MV.propKey, match.getProperty(MV.propKey));
      assertTrue("String[] should be equal",
          Arrays.equals(MV.multiValueA, (String[]) match.getProperty(MV.propKey)));
      found++;
    }
    assertTrue("Should have found only one match; found: " + found, found == 1);
  }
  
  /**
   * search for "x" or "y" find contentX only once
   * 
   * @throws StorageClientException
   * @throws AccessDeniedException
   */
  @Test
  public void testMultiValuedIndexSearchFindXorY() throws StorageClientException,
      AccessDeniedException {
    final ContentManager contentManager = setupMultiValuedIndexSearch();
    final Query orSet = createOrQuery(MV.propField, new String[] { MV.multiValueB[0], MV.multiValueB[1] });
    final Iterable<Content> iterable = contentManager.find(orSet, null);
    assertNotNull("Iterable should not be null", iterable);
    final Iterator<Content> iter = iterable.iterator();
    assertNotNull("Iterator should not be null", iter);
    assertTrue("Should have found a match", iter.hasNext());
    int found = 0;
    while (iter.hasNext()) {
      final Content match = iter.next();
      assertNotNull("match should not be null", match);
      assertEquals(MV.pathB, match.getPath());
      assertNotNull("match should have key: " + MV.propKey, match.getProperty(MV.propKey));
      assertTrue("String[] should be equal",
          Arrays.equals(MV.multiValueB, (String[]) match.getProperty(MV.propKey)));
      found++;
    }
    assertTrue("Should have found only one match; found: " + found, found == 1);
  }

  /**
   * search for "x" or "y" find contentX only once
   * 
   * @throws StorageClientException
   * @throws AccessDeniedException
   */
  @Test
  public void testMultiValuedIndexSearchFindXorZ() throws StorageClientException,
      AccessDeniedException {
    final ContentManager contentManager = setupMultiValuedIndexSearch();
    final Query orSet = createOrQuery(MV.propField, new String[] { MV.multiValueB[0],
        MV.multiValueB[2] });
    final Iterable<Content> iterable = contentManager.find(orSet, null);
    assertNotNull("Iterable should not be null", iterable);
    final Iterator<Content> iter = iterable.iterator();
    assertNotNull("Iterator should not be null", iter);
    assertTrue("Should have found a match", iter.hasNext());
    int found = 0;
    while (iter.hasNext()) {
      final Content match = iter.next();
      assertNotNull("match should not be null", match);
      assertEquals(MV.pathB, match.getPath());
      assertNotNull("match should have key: " + MV.propKey, match.getProperty(MV.propKey));
      assertTrue("String[] should be equal",
          Arrays.equals(MV.multiValueB, (String[]) match.getProperty(MV.propKey)));
      found++;
    }
    assertTrue("Should have found only one match; found: " + found, found == 1);
  }

  /**
   * search for "a" and "b" find contentA only once
   * 
   * @throws StorageClientException
   * @throws AccessDeniedException
   */
  @Test
  public void testMultiValuedIndexSearchFindAandB() throws StorageClientException,
      AccessDeniedException {
    final ContentManager contentManager = setupMultiValuedIndexSearch();
    final Query searchCriteria = createAndQuery(MV.propField, MV.multiValueA);
    final Iterable<Content> iterable = contentManager.find(searchCriteria, null);
    assertNotNull("Iterable should not be null", iterable);
    final Iterator<Content> iter = iterable.iterator();
    assertNotNull("Iterator should not be null", iter);
    assertTrue("Should have found a match", iter.hasNext());
    int found = 0;
    while (iter.hasNext()) {
      final Content match = iter.next();
      assertNotNull("match should not be null", match);
      assertEquals(MV.pathA, match.getPath());
      assertNotNull("match should have key: " + MV.propKey, match.getProperty(MV.propKey));
      assertTrue("String[] should be equal",
          Arrays.equals(MV.multiValueA, (String[]) match.getProperty(MV.propKey)));
      found++;
    }
    assertTrue("Should have found only one match; found: " + found, found == 1);
  }
  
  /**
   * search for "a" and "x" find nothing
   * 
   * @throws StorageClientException
   * @throws AccessDeniedException
   */
  @Test
  public void testMultiValuedIndexSearchFindAandX() throws StorageClientException,
      AccessDeniedException {
    final ContentManager contentManager = setupMultiValuedIndexSearch();
    final Query searchCriteria = createAndQuery(MV.propField, new String[] {
        MV.multiValueA[0], MV.multiValueB[0] });
    final Iterable<Content> iterable = contentManager.find(searchCriteria, null);
    assertNotNull("Iterable should not be null", iterable);
    final Iterator<Content> iter = iterable.iterator();
    assertNotNull("Iterator should not be null", iter);
    assertFalse("Should NOT have found a match", iter.hasNext());
    int found = 0;
    while (iter.hasNext()) {
      found++;
    }
    assertTrue("Should NOT have found any match; found: " + found, found == 0);
  }

  /**
   * search for "a" and "x" find nothing
   * 
   * @throws StorageClientException
   * @throws AccessDeniedException
   */
  @Test
  public void testMultiValuedIndexSearchFindAandX2() throws StorageClientException,
      AccessDeniedException {
    final ContentManager contentManager = setupMultiValuedIndexSearch();
    final Query searchCriteria = createAndQuery(MV.propField, new String[] { MV.multiValueA[1],
        MV.multiValueB[1] });
    final Iterable<Content> iterable = contentManager.find(searchCriteria, null);
    assertNotNull("Iterable should not be null", iterable);
    final Iterator<Content> iter = iterable.iterator();
    assertNotNull("Iterator should not be null", iter);
    assertFalse("Should NOT have found a match", iter.hasNext());
    int found = 0;
    while (iter.hasNext()) {
      found++;
    }
    assertTrue("Should NOT have found any match; found: " + found, found == 0);
  }

  /**
   * search for "a" or "x" find contentA and contentB
   * 
   * @throws StorageClientException
   * @throws AccessDeniedException
   */
  @Test
  public void testMultiValuedIndexSearchFindAorX() throws StorageClientException,
      AccessDeniedException {
    final ContentManager contentManager = setupMultiValuedIndexSearch();
    final Query searchCriteria = createOrQuery(MV.propField, new String[] { MV.multiValueA[0],
        MV.multiValueB[0] });
    final Iterable<Content> iterable = contentManager.find(searchCriteria, null);
    assertNotNull("Iterable should not be null", iterable);
    final Iterator<Content> iter = iterable.iterator();
    assertNotNull("Iterator should not be null", iter);
    assertTrue("Should have found a match", iter.hasNext());
    int found = 0;
    while (iter.hasNext()) {
      final Content match = iter.next();
      assertNotNull("match should not be null", match);
      assertTrue("Path should match one of the two Contents",
          MV.pathA.equals(match.getPath()) || MV.pathB.equals(match.getPath()));
      assertNotNull("match should have key: " + MV.propKey, match.getProperty(MV.propKey));
      assertTrue("Multi-valued property should equal one of the two Contents",
          Arrays.equals(MV.multiValueA, (String[]) match.getProperty(MV.propKey))
              || Arrays.equals(MV.multiValueB, (String[]) match.getProperty(MV.propKey)));
      found++;
    }
    assertTrue("Should have found two matches; found: " + found, found == 2);
  }

  /**
   * search for "a" or "x" find contentA and contentB
   * 
   * @throws StorageClientException
   * @throws AccessDeniedException
   */
  @Test
  public void testMultiValuedIndexSearchFindAorX2() throws StorageClientException,
      AccessDeniedException {
    final ContentManager contentManager = setupMultiValuedIndexSearch();
    final Query searchCriteria = createOrQuery(MV.propField, new String[] {
        MV.multiValueA[1], MV.multiValueB[1] });
    final Iterable<Content> iterable = contentManager.find(searchCriteria, null);
    assertNotNull("Iterable should not be null", iterable);
    final Iterator<Content> iter = iterable.iterator();
    assertNotNull("Iterator should not be null", iter);
    assertTrue("Should have found a match", iter.hasNext());
    int found = 0;
    while (iter.hasNext()) {
      final Content match = iter.next();
      assertNotNull("match should not be null", match);
      assertTrue("Path should match one of the two Contents",
          MV.pathA.equals(match.getPath()) || MV.pathB.equals(match.getPath()));
      assertNotNull("match should have key: " + MV.propKey, match.getProperty(MV.propKey));
      assertTrue("Multi-valued property should equal one of the two Contents",
          Arrays.equals(MV.multiValueA, (String[]) match.getProperty(MV.propKey))
              || Arrays.equals(MV.multiValueB, (String[]) match.getProperty(MV.propKey)));
      found++;
    }
    assertTrue("Should have found two matches; found: " + found, found == 2);
  }

  /**
   * search for "a" or "x" find contentA and contentB
   * 
   * @throws StorageClientException
   * @throws AccessDeniedException
   */
  @Test
  public void testMultiValuedIndexSearchFindAorX3() throws StorageClientException,
      AccessDeniedException {
    final ContentManager contentManager = setupMultiValuedIndexSearch();
    final Query searchCriteria = createOrQuery(MV.propField, new String[] { MV.multiValueA[1],
        MV.multiValueB[2] });
    final Iterable<Content> iterable = contentManager.find(searchCriteria, null);
    assertNotNull("Iterable should not be null", iterable);
    final Iterator<Content> iter = iterable.iterator();
    assertNotNull("Iterator should not be null", iter);
    assertTrue("Should have found a match", iter.hasNext());
    int found = 0;
    while (iter.hasNext()) {
      final Content match = iter.next();
      assertNotNull("match should not be null", match);
      assertTrue("Path should match one of the two Contents",
          MV.pathA.equals(match.getPath()) || MV.pathB.equals(match.getPath()));
      assertNotNull("match should have key: " + MV.propKey, match.getProperty(MV.propKey));
      assertTrue("Multi-valued property should equal one of the two Contents",
          Arrays.equals(MV.multiValueA, (String[]) match.getProperty(MV.propKey))
              || Arrays.equals(MV.multiValueB, (String[]) match.getProperty(MV.propKey)));
      found++;
    }
    assertTrue("Should have found two matches; found: " + found, found == 2);
  }

  /**
   * search for "a" and "x" find nothing
   * 
   * @throws StorageClientException
   * @throws AccessDeniedException
   */
  @Test
  public void testMultiValuedIndexSearchFindAandX3() throws StorageClientException,
      AccessDeniedException {
    final ContentManager contentManager = setupMultiValuedIndexSearch();
    final Query searchCriteria = createAndQuery(MV.propField, new String[] { MV.multiValueA[1],
        MV.multiValueB[2] });
    final Iterable<Content> iterable = contentManager.find(searchCriteria, null);
    assertNotNull("Iterable should not be null", iterable);
    final Iterator<Content> iter = iterable.iterator();
    assertNotNull("Iterator should not be null", iter);
    assertFalse("Should NOT have found a match", iter.hasNext());
    int found = 0;
    while (iter.hasNext()) {
      found++;
    }
    assertTrue("Should NOT have found any match; found: " + found, found == 0);
  }

  /**
   * search for "a" find contentA
   * 
   * @throws StorageClientException
   * @throws AccessDeniedException
   */
  @Test
  public void testMultiValuedIndexSearchFindAltA() throws StorageClientException,
      AccessDeniedException {
    final ContentManager contentManager = setupAlternateMultiValuedProperties();
    final Query searchCriteria = createQuery(MV.propField, MV.altMultiValueA[0]);
    final Iterable<Content> iterable = contentManager.find(searchCriteria, null);
    assertNotNull("Iterable should not be null", iterable);
    final Iterator<Content> iter = iterable.iterator();
    assertNotNull("Iterator should not be null", iter);
    assertTrue("Should have found a match", iter.hasNext());
    int found = 0;
    while (iter.hasNext()) {
      final Content match = iter.next();
      assertNotNull("match should not be null", match);
      assertEquals(MV.pathA, match.getPath());
      assertNotNull("match should have key: " + MV.propKey, match.getProperty(MV.propKey));
      assertTrue("String[] should be equal",
          Arrays.equals(MV.altMultiValueA, (String[]) match.getProperty(MV.propKey)));
      found++;
    }
    assertTrue("Should have found only one match; found: " + found, found == 1);
  }

  /**
   * search for "a" find contentA
   * 
   * @throws StorageClientException
   * @throws AccessDeniedException
   */
  @Test
  public void testMultiValuedIndexSearchFindAltA2() throws StorageClientException,
      AccessDeniedException {
    final ContentManager contentManager = setupAlternateMultiValuedProperties();
    final Query searchCriteria = createQuery(MV.propField, MV.altMultiValueA[1]);
    final Iterable<Content> iterable = contentManager.find(searchCriteria, null);
    assertNotNull("Iterable should not be null", iterable);
    final Iterator<Content> iter = iterable.iterator();
    assertNotNull("Iterator should not be null", iter);
    assertTrue("Should have found a match", iter.hasNext());
    int found = 0;
    while (iter.hasNext()) {
      final Content match = iter.next();
      assertNotNull("match should not be null", match);
      assertEquals(MV.pathA, match.getPath());
      assertNotNull("match should have key: " + MV.propKey, match.getProperty(MV.propKey));
      assertTrue("String[] should be equal",
          Arrays.equals(MV.altMultiValueA, (String[]) match.getProperty(MV.propKey)));
      found++;
    }
    assertTrue("Should have found only one match; found: " + found, found == 1);
  }

  /**
   * search for "a" find contentA
   * 
   * @throws StorageClientException
   * @throws AccessDeniedException
   */
  @Test
  public void testMultiValuedIndexSearchFindAltA3() throws StorageClientException,
      AccessDeniedException {
    final ContentManager contentManager = setupAlternateMultiValuedProperties();
    final Query searchCriteria = createQuery(MV.propField, MV.altMultiValueA[2]);
    final Iterable<Content> iterable = contentManager.find(searchCriteria, null);
    assertNotNull("Iterable should not be null", iterable);
    final Iterator<Content> iter = iterable.iterator();
    assertNotNull("Iterator should not be null", iter);
    assertTrue("Should have found a match", iter.hasNext());
    int found = 0;
    while (iter.hasNext()) {
      final Content match = iter.next();
      assertNotNull("match should not be null", match);
      assertEquals(MV.pathA, match.getPath());
      assertNotNull("match should have key: " + MV.propKey, match.getProperty(MV.propKey));
      assertTrue("String[] should be equal",
          Arrays.equals(MV.altMultiValueA, (String[]) match.getProperty(MV.propKey)));
      found++;
    }
    assertTrue("Should have found only one match; found: " + found, found == 1);
  }

  /**
   * search for "a" or "b" find contentA only once
   * 
   * @throws StorageClientException
   * @throws AccessDeniedException
   */
  @Test
  public void testMultiValuedIndexSearchFindAltAorB() throws StorageClientException,
      AccessDeniedException {
    final ContentManager contentManager = setupAlternateMultiValuedProperties();
    final Query searchCriteria = createOrQuery(MV.propField, new String[] { MV.altMultiValueA[0],
        MV.altMultiValueA[1] });
    final Iterable<Content> iterable = contentManager.find(searchCriteria, null);
    assertNotNull("Iterable should not be null", iterable);
    final Iterator<Content> iter = iterable.iterator();
    assertNotNull("Iterator should not be null", iter);
    assertTrue("Should have found a match", iter.hasNext());
    int found = 0;
    while (iter.hasNext()) {
      final Content match = iter.next();
      assertNotNull("match should not be null", match);
      assertEquals(MV.pathA, match.getPath());
      assertNotNull("match should have key: " + MV.propKey, match.getProperty(MV.propKey));
      assertTrue("String[] should be equal",
          Arrays.equals(MV.altMultiValueA, (String[]) match.getProperty(MV.propKey)));
      found++;
    }
    assertTrue("Should have found only one match; found: " + found, found == 1);
  }

  /**
   * search for "a" or "b" find contentA only once
   * 
   * @throws StorageClientException
   * @throws AccessDeniedException
   */
  @Test
  public void testMultiValuedIndexSearchFindAltAorB2() throws StorageClientException,
      AccessDeniedException {
    final ContentManager contentManager = setupAlternateMultiValuedProperties();
    final Query searchCriteria = createOrQuery(MV.propField, new String[] { MV.altMultiValueA[1],
        MV.altMultiValueA[2] });
    final Iterable<Content> iterable = contentManager.find(searchCriteria, null);
    assertNotNull("Iterable should not be null", iterable);
    final Iterator<Content> iter = iterable.iterator();
    assertNotNull("Iterator should not be null", iter);
    assertTrue("Should have found a match", iter.hasNext());
    int found = 0;
    while (iter.hasNext()) {
      final Content match = iter.next();
      assertNotNull("match should not be null", match);
      assertEquals(MV.pathA, match.getPath());
      assertNotNull("match should have key: " + MV.propKey, match.getProperty(MV.propKey));
      assertTrue("String[] should be equal",
          Arrays.equals(MV.altMultiValueA, (String[]) match.getProperty(MV.propKey)));
      found++;
    }
    assertTrue("Should have found only one match; found: " + found, found == 1);
  }

  /**
   * search for "a" and "b" find contentA only once
   * 
   * @throws StorageClientException
   * @throws AccessDeniedException
   */
  @Test
  public void testMultiValuedIndexSearchFindAltAandB() throws StorageClientException,
      AccessDeniedException {
    final ContentManager contentManager = setupAlternateMultiValuedProperties();
    final Query searchCriteria = createAndQuery(MV.propField, new String[] {
        MV.altMultiValueA[0], MV.altMultiValueA[1] });
    final Iterable<Content> iterable = contentManager.find(searchCriteria, null);
    assertNotNull("Iterable should not be null", iterable);
    final Iterator<Content> iter = iterable.iterator();
    assertNotNull("Iterator should not be null", iter);
    assertTrue("Should have found a match", iter.hasNext());
    int found = 0;
    while (iter.hasNext()) {
      final Content match = iter.next();
      assertNotNull("match should not be null", match);
      assertEquals(MV.pathA, match.getPath());
      assertNotNull("match should have key: " + MV.propKey, match.getProperty(MV.propKey));
      assertTrue("String[] should be equal",
          Arrays.equals(MV.altMultiValueA, (String[]) match.getProperty(MV.propKey)));
      found++;
    }
    assertTrue("Should have found only one match; found: " + found, found == 1);
  }

  /**
   * search for "a" and "x" find nothing
   * 
   * @throws StorageClientException
   * @throws AccessDeniedException
   */
  @Test
  public void testMultiValuedIndexSearchFindAltAandX() throws StorageClientException,
      AccessDeniedException {
    final ContentManager contentManager = setupAlternateMultiValuedProperties();
    final Query searchCriteria = createAndQuery(MV.propKey, new String[] {
        MV.altMultiValueA[0], MV.altMultiValueB[0] });
    final Iterable<Content> iterable = contentManager.find(searchCriteria, null);
    assertNotNull("Iterable should not be null", iterable);
    final Iterator<Content> iter = iterable.iterator();
    assertNotNull("Iterator should not be null", iter);
    assertFalse("Should NOT have found a match", iter.hasNext());
    int found = 0;
    while (iter.hasNext()) {
      found++;
    }
    assertTrue("Should NOT have found any matches; found: " + found, found == 0);
  }

  /**
   * search for "a" or "x" find contentA and contentB
   * 
   * @throws StorageClientException
   * @throws AccessDeniedException
   */
  @Test
  public void testMultiValuedIndexSearchFindAltAorX() throws StorageClientException,
      AccessDeniedException {
    final ContentManager contentManager = setupAlternateMultiValuedProperties();
    final Query searchCriteria = createOrQuery(MV.propField, new String[] { MV.altMultiValueA[0],
        MV.altMultiValueB[0] });
    final Iterable<Content> iterable = contentManager.find(searchCriteria, null);
    assertNotNull("Iterable should not be null", iterable);
    final Iterator<Content> iter = iterable.iterator();
    assertNotNull("Iterator should not be null", iter);
    assertTrue("Should have found a match", iter.hasNext());
    int found = 0;
    while (iter.hasNext()) {
      final Content match = iter.next();
      assertNotNull("match should not be null", match);
      assertTrue("Path should match one of the two Contents",
          MV.pathA.equals(match.getPath()) || MV.pathB.equals(match.getPath()));
      assertNotNull("match should have key: " + MV.propKey, match.getProperty(MV.propKey));
      assertTrue(
          "Multi-valued property should equal one of the two Contents",
          Arrays.equals(MV.altMultiValueA, (String[]) match.getProperty(MV.propKey))
              || Arrays.equals(MV.altMultiValueB,
                  (String[]) match.getProperty(MV.propKey)));
      found++;
    }
    assertTrue("Should have found two matches; found: " + found, found == 2);
  }

  /**
   * search for "a" or "x" find contentA and contentB
   * 
   * @throws StorageClientException
   * @throws AccessDeniedException
   */
  @Test
  public void testMultiValuedIndexSearchFindAltAorX2() throws StorageClientException,
      AccessDeniedException {
    final ContentManager contentManager = setupAlternateMultiValuedProperties();
    final Query searchCriteria = createOrQuery(MV.propField, new String[] { MV.altMultiValueA[1],
        MV.altMultiValueB[1] });
    final Iterable<Content> iterable = contentManager.find(searchCriteria, null);
    assertNotNull("Iterable should not be null", iterable);
    final Iterator<Content> iter = iterable.iterator();
    assertNotNull("Iterator should not be null", iter);
    assertTrue("Should have found a match", iter.hasNext());
    int found = 0;
    while (iter.hasNext()) {
      final Content match = iter.next();
      assertNotNull("match should not be null", match);
      assertTrue("Path should match one of the two Contents",
          MV.pathA.equals(match.getPath()) || MV.pathB.equals(match.getPath()));
      assertNotNull("match should have key: " + MV.propKey, match.getProperty(MV.propKey));
      assertTrue(
          "Multi-valued property should equal one of the two Contents",
          Arrays.equals(MV.altMultiValueA, (String[]) match.getProperty(MV.propKey))
              || Arrays.equals(MV.altMultiValueB,
                  (String[]) match.getProperty(MV.propKey)));
      found++;
    }
    assertTrue("Should have found two matches; found: " + found, found == 2);
  }

  /**
   * search for "a" or "x" find contentA and contentB
   * 
   * @throws StorageClientException
   * @throws AccessDeniedException
   */
  @Test
  public void testMultiValuedIndexSearchFindAltAorX3() throws StorageClientException,
      AccessDeniedException {
    final ContentManager contentManager = setupAlternateMultiValuedProperties();
    final Query searchCriteria = createOrQuery(MV.propField, new String[] { MV.altMultiValueA[2],
        MV.altMultiValueB[1] });
    final Iterable<Content> iterable = contentManager.find(searchCriteria, null);
    assertNotNull("Iterable should not be null", iterable);
    final Iterator<Content> iter = iterable.iterator();
    assertNotNull("Iterator should not be null", iter);
    assertTrue("Should have found a match", iter.hasNext());
    int found = 0;
    while (iter.hasNext()) {
      final Content match = iter.next();
      assertNotNull("match should not be null", match);
      assertTrue("Path should match one of the two Contents",
          MV.pathA.equals(match.getPath()) || MV.pathB.equals(match.getPath()));
      assertNotNull("match should have key: " + MV.propKey, match.getProperty(MV.propKey));
      assertTrue(
          "Multi-valued property should equal one of the two Contents",
          Arrays.equals(MV.altMultiValueA, (String[]) match.getProperty(MV.propKey))
              || Arrays.equals(MV.altMultiValueB,
                  (String[]) match.getProperty(MV.propKey)));
      found++;
    }
    assertTrue("Should have found two matches; found: " + found, found == 2);
  }

  /**
   * search for "x" or "y" find contentX only once
   * 
   * @throws StorageClientException
   * @throws AccessDeniedException
   */
  @Test
  public void testMultiValuedIndexSearchFindAltXorY() throws StorageClientException,
      AccessDeniedException {
    final ContentManager contentManager = setupAlternateMultiValuedProperties();
    final Query searchCriteria = createOrQuery(MV.propField, new String[] { MV.altMultiValueA[0],
        MV.altMultiValueA[1] });
    final Iterable<Content> iterable = contentManager.find(searchCriteria, null);
    assertNotNull("Iterable should not be null", iterable);
    final Iterator<Content> iter = iterable.iterator();
    assertNotNull("Iterator should not be null", iter);
    assertTrue("Should have found a match", iter.hasNext());
    int found = 0;
    while (iter.hasNext()) {
      final Content match = iter.next();
      assertNotNull("match should not be null", match);
      assertEquals(MV.pathA, match.getPath());
      assertNotNull("match should have key: " + MV.propKey, match.getProperty(MV.propKey));
      assertTrue("String[] should be equal",
          Arrays.equals(MV.altMultiValueA, (String[]) match.getProperty(MV.propKey)));
      found++;
    }
    assertTrue("Should have found only one match; found: " + found, found == 1);
  }

  /**
   * search for "x" or "y" find contentX only once
   * 
   * @throws StorageClientException
   * @throws AccessDeniedException
   */
  @Test
  public void testMultiValuedIndexSearchFindAltXorZ() throws StorageClientException,
      AccessDeniedException {
    final ContentManager contentManager = setupAlternateMultiValuedProperties();
    final Query searchCriteria = createOrQuery(MV.propField, new String[] { MV.altMultiValueA[0],
        MV.altMultiValueA[2] });
    final Iterable<Content> iterable = contentManager.find(searchCriteria, null);
    assertNotNull("Iterable should not be null", iterable);
    final Iterator<Content> iter = iterable.iterator();
    assertNotNull("Iterator should not be null", iter);
    assertTrue("Should have found a match", iter.hasNext());
    int found = 0;
    while (iter.hasNext()) {
      final Content match = iter.next();
      assertNotNull("match should not be null", match);
      assertEquals(MV.pathA, match.getPath());
      assertNotNull("match should have key: " + MV.propKey, match.getProperty(MV.propKey));
      assertTrue("String[] should be equal",
          Arrays.equals(MV.altMultiValueA, (String[]) match.getProperty(MV.propKey)));
      found++;
    }
    assertTrue("Should have found only one match; found: " + found, found == 1);
  }

  @Test
  public void testFindAfterChangingPropertyValue() throws Exception {

    String oldValue = "testFindAfterChangingPropertyValue-val1-"+System.currentTimeMillis();
    String newValue = "testFindAfterChangingPropertyValue-newval-"+System.currentTimeMillis();

    AuthenticatorImpl AuthenticatorImpl = new AuthenticatorImpl(client, configuration, null);
    User currentUser = AuthenticatorImpl.authenticate("admin", "admin");
    AccessControlManagerImpl accessControlManager = new AccessControlManagerImpl(client,
            currentUser, configuration, null, new LoggingStorageListener(), principalValidatorResolver);
    ContentManagerImpl contentManager = new ContentManagerImpl(fs, client, accessControlManager,
            configuration, null, new LoggingStorageListener());

    StorageClientUtils.deleteTree(contentManager, "/testFindAfterChangingPropertyValue");

    // create content
    contentManager.update(new Content("/testFindAfterChangingPropertyValue", ImmutableMap.of("sakai:marker", (Object) oldValue)));

    // after initial creation, prop1 should be "val1"
    Iterable<Content> results = contentManager.find(createSimpleMarkerQuery(oldValue), null);
    Iterator<Content> resultsIterator = results.iterator();
    Assert.assertTrue(resultsIterator.hasNext());
    Content found = resultsIterator.next();
    Assert.assertEquals("/testFindAfterChangingPropertyValue", found.getPath());
    Assert.assertEquals(oldValue, found.getProperty("sakai:marker"));

    // now change prop1
    found.setProperty("sakai:marker", newValue);
    contentManager.update(found);

    // calling get() shows prop1 has been updated
    Content gotten = contentManager.get("/testFindAfterChangingPropertyValue");
    Assert.assertEquals(newValue, gotten.getProperty("sakai:marker"));

    // ok, now see if we can find the object searching on "newval"
    Iterable<Content> findOfNewVal = contentManager.find(createSimpleMarkerQuery(newValue), null);
    Content foundAfterUpdate = findOfNewVal.iterator().next();
    Assert.assertEquals(newValue, foundAfterUpdate.getProperty("sakai:marker"));

    // find on the old val should return an empty iterator
    Iterable<Content> findOfOldval = contentManager.find(createSimpleMarkerQuery(oldValue), null);
    // if find() is correct this line should pass
    Assert.assertFalse(findOfOldval.iterator().hasNext());
  }

  /**
   * Test the count functionality of content manager.
   * 
   * @throws StorageClientException
   * @throws AccessDeniedException
   */
  @Test
  public void testCountTest() throws StorageClientException, AccessDeniedException {
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

      Assert.assertEquals(1, contentManager.count(createSimpleMarkerQuery("testSimpleFindvalue4")));
      Assert.assertEquals(2, contentManager.count(createSimpleMarkerQuery("testSimpleFindvalue1")));
  }

  /**
   * Create two contents with default values
   * 
   * @return
   * @throws StorageClientException
   * @throws AccessDeniedException
   */
  private ContentManager setupMultiValuedIndexSearch() throws StorageClientException,
      AccessDeniedException {
    AuthenticatorImpl AuthenticatorImpl = new AuthenticatorImpl(client, configuration, null);
    User currentUser = AuthenticatorImpl.authenticate("admin", "admin");

    AccessControlManagerImpl accessControlManager = new AccessControlManagerImpl(client,
        currentUser, configuration, null, new LoggingStorageListener(),
        principalValidatorResolver);
    ContentManager contentManager = new ContentManagerImpl(fs, client, accessControlManager,
        configuration, null, new LoggingStorageListener());
    // add some content with multi-valued properties
    Content contentA = contentManager.get(MV.pathA);
    if (contentA == null) {
      contentManager.update(new Content(MV.pathA, ImmutableMap.of(MV.propKey,
          (Object) MV.multiValueA)));
    } else {
      contentA.setProperty(MV.propKey, (Object) MV.multiValueA);
      contentManager.update(contentA);
    }
    Content contentX = contentManager.get(MV.pathB);
    if (contentX == null) {
      contentManager.update(new Content(MV.pathB, ImmutableMap.of(MV.propKey,
          (Object) MV.multiValueB)));
    } else {
      contentX.setProperty(MV.propKey, (Object) MV.multiValueB);
      contentManager.update(contentX);
    }

    // get the content.
    contentA = contentManager.get(MV.pathA);
    contentX = contentManager.get(MV.pathB);

    // force a second update to ensure that the fields have been written more than once,
    // if there is a problem this will cause the tests to fail when they are run in a batch or
    // individually.
    contentX.setProperty(MV.propKey, (Object) MV.multiValueB);
    contentManager.update(contentX);
    contentA.setProperty(MV.propKey, (Object) MV.multiValueA);
    contentManager.update(contentA);
    
    // verify state of content
    contentA = contentManager.get(MV.pathA);
    contentX = contentManager.get(MV.pathB);


    assertEquals(MV.pathA, contentA.getPath());
    assertEquals(MV.pathB, contentX.getPath());
    Map<String, Object> propsA = contentA.getProperties();
    Map<String, Object> propsX = contentX.getProperties();
    assertTrue(Arrays.equals(MV.multiValueA, (String[]) propsA.get(MV.propKey)));
    assertTrue(Arrays.equals(MV.multiValueB, (String[]) propsX.get(MV.propKey)));
    
    return contentManager;
  }
  
  /**
   * Change the values of the properties to something else
   * 
   * @return
   * @throws StorageClientException
   * @throws AccessDeniedException
   */
  private ContentManager setupAlternateMultiValuedProperties()
      throws StorageClientException, AccessDeniedException {
    ContentManager contentManager = setupMultiValuedIndexSearch();
    // set some alternate multi-valued properties
    Content contentA = contentManager.get(MV.pathA);
    contentA.setProperty(MV.propKey, (Object) MV.altMultiValueA);
    contentManager.update(contentA);
    Content contentX = contentManager.get(MV.pathB);
    contentX.setProperty(MV.propKey, (Object) MV.altMultiValueB);
    contentManager.update(contentX);

    // verify state of content
    contentA = contentManager.get(MV.pathA);
    contentX = contentManager.get(MV.pathB);
    assertEquals(MV.pathA, contentA.getPath());
    assertEquals(MV.pathB, contentX.getPath());
    Map<String, Object> propsA = contentA.getProperties();
    Map<String, Object> propsX = contentX.getProperties();
    Assert
        .assertTrue(Arrays.equals(MV.altMultiValueA, (String[]) propsA.get(MV.propKey)));
    Assert
        .assertTrue(Arrays.equals(MV.altMultiValueB, (String[]) propsX.get(MV.propKey)));
    return contentManager;
  }

  private Query createQuery(String field, String value) {
    return new TermQuery(new Term(field, value));
  }
  
  private Query createOrQuery(String field, String[] values) {
    BooleanQuery bool = new BooleanQuery();
    for (String value : values) {
      bool.add(createQuery(field, value), Occur.SHOULD);
    }
    return bool;
  }
  
  private Query createAndQuery(String field, String[] values) {
    BooleanQuery bool = new BooleanQuery();
    for (String value : values) {
      bool.add(createQuery(field, value), Occur.MUST);
    }
    return bool;
  }
  
  private Query createCategoryQuery(String categoryValue) {
    return new TermQuery(new Term("category", categoryValue));
  }
  
  private Query createSimpleMarkerQuery(String markerValue) {
    return new TermQuery(new Term("marker", markerValue));
  }
  
  private Sort createSimpleSort(String sortOn) {
    return new Sort(new SortField(sortOn, SortField.STRING));
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
      String[] category = (String[]) properties.get("sakai:category");
      if (marker != null || category != null) {
        SampleTestIndexDocument doc = new SampleTestIndexDocument();
        doc.id = path;
        doc.marker = marker;
        doc.category = category;
        return doc;
      }
      return null;
    }
  }

  private static class MV {
    private static final String propField = "category";
    private static final String propKey = "sakai:category";
    private static final String pathA = "/multi/pathA";
    private static final String pathB = "/multi/pathB";
    private static final String[] multiValueA = new String[] { "valueA", "valueB" };
    private static final String[] multiValueB = new String[] { "valueX", "valueY",
        "valueZ" };
    private static final String[] altMultiValueA = multiValueB;
    private static final String[] altMultiValueB = multiValueA;
  }

}
