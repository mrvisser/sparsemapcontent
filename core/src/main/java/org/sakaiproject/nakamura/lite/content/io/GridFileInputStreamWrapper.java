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
package org.sakaiproject.nakamura.lite.content.io;

import java.io.IOException;
import java.io.InputStream;

/**
 *
 */
public class GridFileInputStreamWrapper extends InputStream {

  private final InputStream delegate;
  
  public GridFileInputStreamWrapper(InputStream is) {
    this.delegate = is;
  }
  
  /**
   * {@inheritDoc}
   * @see java.io.InputStream#available()
   */
  @Override
  public int available() throws IOException {
    // do not delegate to the grid-file version of this -- it throws an exception
    return super.available();
  }

  /**
   * {@inheritDoc}
   * @see java.io.InputStream#close()
   */
  @Override
  public void close() throws IOException {
    delegate.close();
  }

  /**
   * {@inheritDoc}
   * @see java.io.InputStream#mark(int)
   */
  @Override
  public synchronized void mark(int readlimit) {
    delegate.mark(readlimit);
  }

  /**
   * {@inheritDoc}
   * @see java.io.InputStream#markSupported()
   */
  @Override
  public boolean markSupported() {
    return delegate.markSupported();
  }

  /**
   * {@inheritDoc}
   * @see java.io.InputStream#read(byte[], int, int)
   */
  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    return delegate.read(b, off, len);
  }

  /**
   * {@inheritDoc}
   * @see java.io.InputStream#read(byte[])
   */
  @Override
  public int read(byte[] b) throws IOException {
    return delegate.read(b);
  }

  /**
   * {@inheritDoc}
   * @see java.io.InputStream#reset()
   */
  @Override
  public synchronized void reset() throws IOException {
    delegate.reset();
  }

  /**
   * {@inheritDoc}
   * @see java.io.InputStream#skip(long)
   */
  @Override
  public long skip(long n) throws IOException {
    return delegate.skip(n);
  }

  /**
   * {@inheritDoc}
   * @see java.io.InputStream#read()
   */
  @Override
  public int read() throws IOException {
    return delegate.read();
  }

}
