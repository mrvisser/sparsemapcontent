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
package org.sakaiproject.nakamura.lite.authorizable;

import com.google.common.collect.ImmutableMap;

import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Service;
import org.sakaiproject.nakamura.api.lite.IndexDocument;
import org.sakaiproject.nakamura.api.lite.IndexDocumentFactory;
import org.sakaiproject.nakamura.api.lite.authorizable.Authorizable;

import java.util.Map;

/**
 * A factory that creates {@code AuthorizableIndexDocument} objects from arbitrary maps of content.
 */
@Component
@Service
public class AuthorizableIndexDocumentFactory implements IndexDocumentFactory {

  private static final Map<String, String> PROPERTY_MAPPING = ImmutableMap.<String, String>of(
      "rep:principalName", "principalName");
  
  /**
   * {@inheritDoc}
   * @see org.sakaiproject.nakamura.api.lite.IndexDocumentFactory#createIndexDocument(java.lang.String, java.util.Map)
   */
  public IndexDocument createIndexDocument(String key, Map<String, Object> properties) {
    if (!isAuthorizableObject(properties))
      return null;
    AuthorizableIndexDocument doc = new AuthorizableIndexDocument();
    doc.id = key;
    doc.type = (String) properties.get(Authorizable.AUTHORIZABLE_TYPE_FIELD);
    doc.principalName = (String) properties.get("rep:principalName");
    return doc;
  }

  /**
   * Given an indexed document property, return the content property name that is mapped to that
   * field.
   * 
   * @param contentProperty
   * @return
   */
  public String getDocumentProperty(String contentProperty) {
    return PROPERTY_MAPPING.get(contentProperty);
  }
  
  /**
   * Determine if the given map describes an authorizable.
   * 
   * @param properties
   * @return
   */
  private boolean isAuthorizableObject(Map<String, Object> properties) {
    String type = (String) properties.get(Authorizable.AUTHORIZABLE_TYPE_FIELD);
    return Authorizable.USER_VALUE.equals(type) || Authorizable.GROUP_VALUE.equals(type);
  }
}
