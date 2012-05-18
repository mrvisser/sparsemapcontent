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

import org.infinispan.loaders.keymappers.Key2StringMapper;
import org.infinispan.tree.NodeKey;

/**
 *
 */
public class NodeKeyKey2StringMapper implements Key2StringMapper {

  /**
   * {@inheritDoc}
   * @see org.infinispan.loaders.keymappers.Key2StringMapper#isSupportedType(java.lang.Class)
   */
  public boolean isSupportedType(Class<?> keyType) {
    return keyType.equals(NodeKey.class);
  }

  /**
   * {@inheritDoc}
   * @see org.infinispan.loaders.keymappers.Key2StringMapper#getStringMapping(java.lang.Object)
   */
  public String getStringMapping(Object key) {
    // the toString() of NodeKey is both the path and type (structure or content) which
    // is also how the .equals() is handled, so this should be safe as a string key.
    return (key != null) ? ((NodeKey) key).toString() : null;
  }

}
