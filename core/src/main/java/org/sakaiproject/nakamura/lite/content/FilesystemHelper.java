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

import org.apache.commons.io.IOUtils;
import org.infinispan.Cache;
import org.infinispan.io.GridFile.Metadata;
import org.infinispan.io.GridFilesystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * A helper class to assist in performing complex file-system operations.
 * 
 * <b>Important:</b>Note that running "list()" or "listFiles" is quite expensive, so we
 * cannot perform recursive algorithms in that sense. We are better off iterating through
 * every path known by the file metadata cache and grabbing all grandchildren just once.
 */
public class FilesystemHelper {

  private final static Logger LOGGER = LoggerFactory.getLogger(FilesystemHelper.class);
  
  private GridFilesystem fs;
  private Cache<String, Metadata> meta;
  
  public FilesystemHelper(GridFilesystem fs, Cache<String, Metadata> meta) {
    this.fs = fs;
    this.meta = meta;
  }
  
  /**
   * Permanently and recursively delete the file at given {@code filesystemPath} and all its
   * grand children. 
   * 
   * @param fs
   * @param filesystemPath
   */
  public void deleteAll(String filesystemPath) {
    assertNotNull(fs, "fs cannot be null");
    assertNotNull(filesystemPath, "dir cannot be null");
    
    File file = fs.getFile(filesystemPath);
    if (file.isDirectory()) {
      List<String> grandchildPaths = findAllGrandchildren(file);
      
      // sort by length desc so that leaf files are deleted first, then eventually their parents
      Collections.sort(grandchildPaths, createStringLengthComparator(false));
      
      // delete all in order of length
      for (String grandchildPath : grandchildPaths) {
        fs.getFile(grandchildPath).delete();
      }
    }
    
    file.delete();
  }
  
  /**
   * Copy the file at {@code fromFilesystemPath} to the {@code toFilesystemPath} location. If the
   * destination location already exists, it will be overwritten. If it does not exist, it will be
   * created.
   * 
   * @param fs
   * @param fromFilesystemPath
   * @param toFilesystemPath
   * @throws IOException
   */
  public void copyFile(String fromFilesystemPath, String toFilesystemPath)
      throws IOException {
    assertNotNull(fs, "fs cannot be null");
    assertNotNull(fromFilesystemPath, "source path cannot be null");
    assertNotNull(toFilesystemPath, "destination path cannot be null");
    
    File fromFile = fs.getFile(fromFilesystemPath);
    assertTrue(fromFile.exists(), "source file does not exist");
    assertFalse(fromFile.isDirectory(), "source file cannot be a directory");
    
    File toFile = fs.getFile(toFilesystemPath);
    assertFalse(toFile.exists() && toFile.isDirectory(),
        "if destination file exists, it cannot be a directory");
    
    // ensure the file exists prior to streaming
    toFile.getParentFile().mkdirs();
    toFile.createNewFile();
    
    InputStream in = null;
    OutputStream out = null;
    
    try {
      in = fs.getInput(fromFilesystemPath);
      out = fs.getOutput(toFilesystemPath);
      IOUtils.copyLarge(in, out);
    } finally {
      closeSilent(in);
      closeSilent(out);
    }
  }
  
  /**
   * Recursively copy the file or directory
   * @param fs
   * @param fromFilesystemPath
   * @param toFilesystemPath
   * @throws IOException
   */
  public void copyAll(String fromFilesystemPath, String toFilesystemPath)
      throws IOException {
    assertNotNull(fs, "fs cannot be null");
    assertNotNull(fromFilesystemPath, "source path cannot be null");
    assertNotNull(toFilesystemPath, "destination path cannot be null");
    
    File from = fs.getFile(fromFilesystemPath);
    assertTrue(from.exists(), "source file does not exist");
    
    if (from.isDirectory()) {
      fs.getFile(toFilesystemPath).mkdirs();
      List<String> allFromPaths = findAllGrandchildren(from);
      
      // start copying at the top and work our way down
      Collections.sort(allFromPaths, createStringLengthComparator(true));
      
      for (String fromChildPath : allFromPaths) {
        String toChildPath = toFilesystemPath.concat(fromChildPath.substring(fromFilesystemPath.length()));
        File fromChild = fs.getFile(fromChildPath);
        File toChild = fs.getFile(toChildPath);
        if (fromChild.isDirectory()) {
          toChild.mkdir();
        } else {
          copyFile(fromChildPath, toChildPath);
        }
      }
    } else {
      copyFile(fromFilesystemPath, toFilesystemPath);
    }
  }

  private List<String> findAllGrandchildren(File file) {
    List<String> grandchildPaths = new LinkedList<String>();
    Set<String> paths = meta.keySet();
    String prefix = String.format("%s/", file.getAbsolutePath());
    for (String path : paths) {
      if (path.startsWith(prefix)) {
        grandchildPaths.add(path);
      }
    }
    return grandchildPaths;
  }
  
  private void assertTrue(boolean b, String message) {
    if (!b) {
      throw new IllegalArgumentException(message);
    }
  }
  
  private void assertFalse(boolean b, String message) {
    assertTrue(!b, message);
  }
  
  private void assertNotNull(Object obj, String message) {
    if (obj == null) {
      throw new IllegalArgumentException(message);
    }
  }
  
  private void closeSilent(InputStream is) {
    try {
      if (is != null) {
        is.close();
      }
    } catch (IOException e) {
      LOGGER.warn("Failed to close input stream.", e);
    }
  }
  
  private void closeSilent(OutputStream os) {
    try {
      if (os != null) {
        os.close();
      }
    } catch (IOException e) {
      LOGGER.warn("Failed to close output stream.", e);
    }
  }
  
  private Comparator<String> createStringLengthComparator(boolean asc) {
    final int mp = asc ? 1 : -1;
    return new Comparator<String>() {
      public int compare(String one, String other) {
        return mp*Integer.valueOf(one.length()).compareTo(other.length());
      }
    };
  }
}
