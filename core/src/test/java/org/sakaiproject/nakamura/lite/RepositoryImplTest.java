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

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.sakaiproject.nakamura.api.lite.ClientPoolException;
import org.sakaiproject.nakamura.api.lite.Session;
import org.sakaiproject.nakamura.api.lite.StorageClientException;
import org.sakaiproject.nakamura.api.lite.accesscontrol.AccessDeniedException;
import org.sakaiproject.nakamura.api.lite.authorizable.AuthorizableManager;
import org.sakaiproject.nakamura.api.lite.authorizable.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class RepositoryImplTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(RepositoryImplTest.class);
    private RepositoryImpl repository;

    @Before
    public void before() throws ClientPoolException, StorageClientException, AccessDeniedException,
        ClassNotFoundException, IOException {
      repository = (new BaseMemoryRepository()).getRepository();
    }
    
    @After
    public void after() throws ClientPoolException {
        repository.deactivate(null);
    }

    @Test
    public void testStart() throws ClientPoolException, StorageClientException,
            AccessDeniedException, ClassNotFoundException, IOException {
        Session session = repository.loginAdministrative();
        Assert.assertEquals(User.ADMIN_USER, session.getUserId());
        AuthorizableManager am = session.getAuthorizableManager();
        am.delete("testuser");
        am.createUser("testuser", "Test User", "test",
                ImmutableMap.of("UserName", (Object) "User Name"));
        session.logout();

        session = repository.login("testuser", "test");
        Assert.assertEquals("testuser", session.getUserId());
        Assert.assertNotNull(session.getAccessControlManager());
        Assert.assertNotNull(session.getAuthorizableManager());
        Assert.assertNotNull(session.getContentManager());
        session.logout();

        session = repository.login();
        Assert.assertEquals(User.ANON_USER, session.getUserId());
        Assert.assertNotNull(session.getAccessControlManager());
        Assert.assertNotNull(session.getAuthorizableManager());
        Assert.assertNotNull(session.getContentManager());
        session.logout();

    }
    
    @Test
    public void testBlah() {
      Object obj = new Object();
      obj = Mockito.spy(obj);
      Mockito.when(obj.toString()).thenReturn("my to string!");
      Assert.assertEquals("my to string!", obj.toString());
    }

}
