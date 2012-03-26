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
import org.infinispan.io.GridFilesystem;
import org.sakaiproject.nakamura.api.lite.StorageClientUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * A helper class to assist in performing complex file-system operations.
 */
public class FilesystemHelper {

  private final static Logger LOGGER = LoggerFactory.getLogger(FilesystemHelper.class);
  
  /**
   * Permanently and recursively delete the file at given {@code filesystemPath} and all its
   * children. 
   * 
   * @param fs
   * @param filesystemPath
   */
  public static void deleteAll(GridFilesystem fs, String filesystemPath) {
    assertNotNull(fs, "fs cannot be null");
    assertNotNull(filesystemPath, "dir cannot be null");
    
    File file = fs.getFile(filesystemPath);
    if (file.isDirectory()) {
      // recursively delete directory children
      for (File child : file.listFiles()) {
        deleteAll(fs, child.getAbsolutePath());
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
  public static void copyFile(GridFilesystem fs, String fromFilesystemPath,
      String toFilesystemPath) throws IOException {
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
  public static void copyAll(GridFilesystem fs, String fromFilesystemPath,
      String toFilesystemPath) throws IOException {
    assertNotNull(fs, "fs cannot be null");
    assertNotNull(fromFilesystemPath, "source path cannot be null");
    assertNotNull(toFilesystemPath, "destination path cannot be null");
    
    File from = fs.getFile(fromFilesystemPath);
    assertTrue(from.exists(), "source file does not exist");
    
    if (from.isDirectory()) {
      fs.getFile(toFilesystemPath).mkdirs();
      for (File child : from.listFiles()) {
        String childToPath = StorageClientUtils.newPath(toFilesystemPath, child.getName());
        copyAll(fs, child.getAbsolutePath(), childToPath);
      }
    } else {
      copyFile(fs, fromFilesystemPath, toFilesystemPath);
    }
  }

  private static void assertTrue(boolean b, String message) {
    if (!b) {
      throw new IllegalArgumentException(message);
    }
  }
  
  private static void assertFalse(boolean b, String message) {
    assertTrue(!b, message);
  }
  
  private static void assertNotNull(Object obj, String message) {
    if (obj == null) {
      throw new IllegalArgumentException(message);
    }
  }
  
  private static void closeSilent(InputStream is) {
    try {
      if (is != null) {
        is.close();
      }
    } catch (IOException e) {
      LOGGER.warn("Failed to close input stream.", e);
    }
  }
  
  private static void closeSilent(OutputStream os) {
    try {
      if (os != null) {
        os.close();
      }
    } catch (IOException e) {
      LOGGER.warn("Failed to close output stream.", e);
    }
  }
}
