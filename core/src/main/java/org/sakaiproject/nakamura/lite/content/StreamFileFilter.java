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

/**
 * A file filter to help identify what is an internal stream file.
 */
public class StreamFileFilter implements FileFilter {

  private String streamNamePrefix;
  
  public StreamFileFilter(String streamNamePrefix) {
    this.streamNamePrefix = streamNamePrefix;
  }
  
  /* (non-Javadoc)
   * 
   * Determines whether or not the given file represents a binary content stream.
   * 
   * @see java.io.FileFilter#accept(java.io.File)
   */
  public boolean accept(File pathname) {
    return !pathname.isDirectory() && pathname.getName().startsWith(streamNamePrefix);
  }
  
  /**
   * Get the target streamId from the given stream file name.
   * 
   * @param fileName
   * @return
   */
  public String getStreamId(String fileName) {
    return fileName.replace(streamNamePrefix, "");
  }
}

