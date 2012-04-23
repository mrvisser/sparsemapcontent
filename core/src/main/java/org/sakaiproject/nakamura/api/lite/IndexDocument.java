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

import java.io.Serializable;

/**
 *
 */
public interface IndexDocument extends Serializable {
  
  /**
   * @return the id that was set by executing {@link #setId()}
   */
  String getId();
  
  /**
   * Set the id of the IndexDocument. This invocation must set the stored ID of the index document
   * such that any subsequent calls to {@link #getId()} result in what was set here, either
   * immediately after invocation, or when the document is retrieved through a search.
   * 
   * In other words, when implementing this interface, make sure that the {@code id} field is
   * a stored field, and ensure that {@link #setId(String)} and {@link #getId()} set and get
   * the {@code id} field, respectively.
   */
  void setId(String id);
}
