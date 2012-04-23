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
package org.sakaiproject.nakamura.api.lite;

import org.sakaiproject.nakamura.api.lite.content.ContentManager;

import java.io.Serializable;
import java.util.Map;

/**
 *
 */
public interface IndexDocumentFactory {

  /**
   * Create an index document from the given {@code path} and content {@code properties}.
   * 
   * @param path The path to the content being indexed.
   * @param properties The properties of the content being indexed.
   * @return An index document that can be indexed in lucene.
   */
  IndexDocument createIndexDocument(String path, Map<String, Object> properties);
  
  /**
   * For a given content property key, provide the name of the field (bean notation) that is mapped
   * to the property within this document. If the requested content property is not mapped (i.e.,
   * it is not indexed), simply return null.
   * 
   * This is only used to reverse-engineer the current API for finding content on the
   * {@link ContentManager#find(Map)} method. If this changes to allow richer lucene searching
   * capabilities, then this method may be obsolete.
   * 
   * @param contentProperty The content field with which to look up the index document property name.
   * @return The index document property name that is associated to the content property.
   */
  String getFieldForContentProperty(String contentProperty);
}
