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

import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;

import javax.naming.Binding;
import javax.naming.Context;
import javax.naming.Name;
import javax.naming.NameClassPair;
import javax.naming.NameParser;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;

/**
 *
 */
public class DummyJndiContext implements Context {

  private static final Map<String, Object> ctx = new HashMap<String, Object>();
  
  /**
   * {@inheritDoc}
   * @see javax.naming.Context#addToEnvironment(java.lang.String, java.lang.Object)
   */
  public Object addToEnvironment(String propName, Object propVal) throws NamingException {
    throw new UnsupportedOperationException();
  }

  /**
   * {@inheritDoc}
   * @see javax.naming.Context#bind(javax.naming.Name, java.lang.Object)
   */
  public void bind(Name name, Object obj) throws NamingException {
    throw new UnsupportedOperationException();
  }

  /**
   * {@inheritDoc}
   * @see javax.naming.Context#bind(java.lang.String, java.lang.Object)
   */
  public void bind(String name, Object obj) throws NamingException {
    ctx.put(name, obj);
  }

  /**
   * {@inheritDoc}
   * @see javax.naming.Context#close()
   */
  public void close() throws NamingException {
    // do nothing
  }

  /**
   * {@inheritDoc}
   * @see javax.naming.Context#composeName(javax.naming.Name, javax.naming.Name)
   */
  public Name composeName(Name name, Name prefix) throws NamingException {
    throw new UnsupportedOperationException();
  }

  /**
   * {@inheritDoc}
   * @see javax.naming.Context#composeName(java.lang.String, java.lang.String)
   */
  public String composeName(String name, String prefix) throws NamingException {
    throw new UnsupportedOperationException();
  }

  /**
   * {@inheritDoc}
   * @see javax.naming.Context#createSubcontext(javax.naming.Name)
   */
  public Context createSubcontext(Name name) throws NamingException {
    throw new UnsupportedOperationException();
  }

  /**
   * {@inheritDoc}
   * @see javax.naming.Context#createSubcontext(java.lang.String)
   */
  public Context createSubcontext(String name) throws NamingException {
    throw new UnsupportedOperationException();
  }

  /**
   * {@inheritDoc}
   * @see javax.naming.Context#destroySubcontext(javax.naming.Name)
   */
  public void destroySubcontext(Name name) throws NamingException {
    throw new UnsupportedOperationException();
  }

  /**
   * {@inheritDoc}
   * @see javax.naming.Context#destroySubcontext(java.lang.String)
   */
  public void destroySubcontext(String name) throws NamingException {
    throw new UnsupportedOperationException();
  }

  /**
   * {@inheritDoc}
   * @see javax.naming.Context#getEnvironment()
   */
  public Hashtable<?, ?> getEnvironment() throws NamingException {
    throw new UnsupportedOperationException();
  }

  /**
   * {@inheritDoc}
   * @see javax.naming.Context#getNameInNamespace()
   */
  public String getNameInNamespace() throws NamingException {
    throw new UnsupportedOperationException();
  }

  /**
   * {@inheritDoc}
   * @see javax.naming.Context#getNameParser(javax.naming.Name)
   */
  public NameParser getNameParser(Name name) throws NamingException {
    throw new UnsupportedOperationException();
  }

  /**
   * {@inheritDoc}
   * @see javax.naming.Context#getNameParser(java.lang.String)
   */
  public NameParser getNameParser(String name) throws NamingException {
    throw new UnsupportedOperationException();
  }

  /**
   * {@inheritDoc}
   * @see javax.naming.Context#list(javax.naming.Name)
   */
  public NamingEnumeration<NameClassPair> list(Name name) throws NamingException {
    throw new UnsupportedOperationException();
  }

  /**
   * {@inheritDoc}
   * @see javax.naming.Context#list(java.lang.String)
   */
  public NamingEnumeration<NameClassPair> list(String name) throws NamingException {
    throw new UnsupportedOperationException();
  }

  /**
   * {@inheritDoc}
   * @see javax.naming.Context#listBindings(javax.naming.Name)
   */
  public NamingEnumeration<Binding> listBindings(Name name) throws NamingException {
    throw new UnsupportedOperationException();
  }

  /**
   * {@inheritDoc}
   * @see javax.naming.Context#listBindings(java.lang.String)
   */
  public NamingEnumeration<Binding> listBindings(String name) throws NamingException {
    throw new UnsupportedOperationException();
  }

  /**
   * {@inheritDoc}
   * @see javax.naming.Context#lookup(javax.naming.Name)
   */
  public Object lookup(Name name) throws NamingException {
    throw new UnsupportedOperationException();
  }

  /**
   * {@inheritDoc}
   * @see javax.naming.Context#lookup(java.lang.String)
   */
  public Object lookup(String name) throws NamingException {
    return ctx.get(name);
  }

  /**
   * {@inheritDoc}
   * @see javax.naming.Context#lookupLink(javax.naming.Name)
   */
  public Object lookupLink(Name name) throws NamingException {
    throw new UnsupportedOperationException();
  }

  /**
   * {@inheritDoc}
   * @see javax.naming.Context#lookupLink(java.lang.String)
   */
  public Object lookupLink(String name) throws NamingException {
    throw new UnsupportedOperationException();
  }

  /**
   * {@inheritDoc}
   * @see javax.naming.Context#rebind(javax.naming.Name, java.lang.Object)
   */
  public void rebind(Name name, Object obj) throws NamingException {
    throw new UnsupportedOperationException();
  }

  /**
   * {@inheritDoc}
   * @see javax.naming.Context#rebind(java.lang.String, java.lang.Object)
   */
  public void rebind(String name, Object obj) throws NamingException {
    throw new UnsupportedOperationException();
  }

  /**
   * {@inheritDoc}
   * @see javax.naming.Context#removeFromEnvironment(java.lang.String)
   */
  public Object removeFromEnvironment(String propName) throws NamingException {
    throw new UnsupportedOperationException();
  }

  /**
   * {@inheritDoc}
   * @see javax.naming.Context#rename(javax.naming.Name, javax.naming.Name)
   */
  public void rename(Name oldName, Name newName) throws NamingException {
    throw new UnsupportedOperationException();
  }

  /**
   * {@inheritDoc}
   * @see javax.naming.Context#rename(java.lang.String, java.lang.String)
   */
  public void rename(String oldName, String newName) throws NamingException {
    throw new UnsupportedOperationException();
  }

  /**
   * {@inheritDoc}
   * @see javax.naming.Context#unbind(javax.naming.Name)
   */
  public void unbind(Name name) throws NamingException {
    throw new UnsupportedOperationException();
  }

  /**
   * {@inheritDoc}
   * @see javax.naming.Context#unbind(java.lang.String)
   */
  public void unbind(String name) throws NamingException {
    ctx.remove(name);
  }

}
