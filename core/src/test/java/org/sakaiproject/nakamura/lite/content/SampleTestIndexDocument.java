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

import org.apache.lucene.analysis.KeywordAnalyzer;
import org.hibernate.search.annotations.Analyzer;
import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.FieldBridge;
import org.hibernate.search.annotations.Index;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.annotations.ProvidedId;
import org.hibernate.search.annotations.Store;
import org.hibernate.search.bridge.builtin.impl.BuiltinArrayBridge;
import org.sakaiproject.nakamura.api.lite.IndexDocument;

/**
 * Index document used to test searches in this test suite.
 */
@Indexed(index="oae")
@ProvidedId
@Analyzer(impl=KeywordAnalyzer.class)
public class SampleTestIndexDocument implements IndexDocument {
  private static final long serialVersionUID = 6667650051279739243L;

  @Field(index=Index.NO,store=Store.YES)
  public String id;
  
  @Field
  public String marker;
  
  @Field @FieldBridge(impl=BuiltinArrayBridge.class)
  public String[] category;
  
  public String getId() {
    return this.id;
  }

  public void setId(String id) {
    this.id = id;
  }
}