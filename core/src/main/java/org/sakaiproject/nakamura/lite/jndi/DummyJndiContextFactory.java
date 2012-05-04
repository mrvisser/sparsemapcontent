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
package org.sakaiproject.nakamura.lite.jndi;

import java.util.Hashtable;

import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.spi.InitialContextFactory;

/**
 * A dummy JNDI context to produce the infinispan cache container for hibernate search.
 * This is referenced in RepositoryImpl, when specifying the JNDI contextfactory class
 * name.
 */
public class DummyJndiContextFactory implements InitialContextFactory {

  private static final Context ic = new DummyJndiContext();
  
  /**
   * {@inheritDoc}
   * @see javax.naming.spi.InitialContextFactory#getInitialContext(java.util.Hashtable)
   */
  public Context getInitialContext(Hashtable<?, ?> props) throws NamingException {
    return ic;
  }

}
