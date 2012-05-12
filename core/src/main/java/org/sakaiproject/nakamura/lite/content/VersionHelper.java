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

import org.apache.commons.lang.StringUtils;
import org.infinispan.tree.Fqn;

/**
 *
 */
public class VersionHelper {

  private static String PREFIX_VERSION = "oae#version#";
  
  public static boolean isVersionNode(String nodeName) {
    if (StringUtils.isBlank(nodeName))
      return false;
    
    nodeName = Fqn.fromString(nodeName).getLastElementAsString();
    return nodeName.startsWith(PREFIX_VERSION);
  }
  
  public static int getVersionNumberFromNodeName(String nodeName) {
    if (!isVersionNode(nodeName))
      return -1;
    return Integer.valueOf(nodeName.substring(PREFIX_VERSION.length()));
  }
  
  public static String getVersionNodeName(int n) {
    return String.format("%s%s", PREFIX_VERSION, String.valueOf(n));
  }
}
