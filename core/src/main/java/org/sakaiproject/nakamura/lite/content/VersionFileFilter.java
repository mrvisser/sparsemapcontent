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

import java.io.File;
import java.io.FileFilter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * A utility class that helps identify files that are version stores, and helps parse the
 * required information out of that version file name.
 */
public class VersionFileFilter implements FileFilter {

  private final String regex;
  private final Pattern versionPattern;
  
  
  public VersionFileFilter(String versionNamePrefix) {
    this.regex = String.format("^%s:([0-9]+)$", versionNamePrefix);
    this.versionPattern = Pattern.compile(regex);
  }
  
  /* (non-Javadoc)
   * 
   * Determine whether or not the given file is a version store for another file.
   *  
   * @see java.io.FileFilter#accept(java.io.File)
   */
  public boolean accept(File pathname) {
    String name = pathname.getName();
    return versionPattern.matcher(name).matches();
  }
  
  /**
   * Get the version number the given filename represents.
   * 
   * @param fileName
   * @return
   */
  public Integer getVersionNumber(String fileName) {
    Matcher m = versionPattern.matcher(fileName);
    if (m.matches()) {
      return Integer.valueOf(m.group(1));
    }
    return null;
  }
}