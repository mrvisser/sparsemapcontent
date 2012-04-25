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
package org.sakaiproject.nakamura.lite.storage.spi;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.infinispan.query.QueryIterator;
import org.sakaiproject.nakamura.api.lite.Repository;
import org.sakaiproject.nakamura.api.lite.StorageClientException;

import java.util.Map;

/**
 * Implementations of the SPI need to implement a {@link StorageClientPool} that
 * pools {@link StorageClient}s.
 * 
 * @author ieb
 * 
 */
public interface StorageClient {

	/**
     * Where an object is deleted, in the repository but still exists in the storage
     * It will be marked with  "Y" in the deleted field. 
     * @since 1.5
     */
    public static final String DELETED_FIELD = Repository.SYSTEM_PROP_PREFIX + "deleted";
    /**
     * true, for above.
     * @since 1.5
     */
    public static final String TRUE = "Y";

    /**
	 * Lookup an object by key
	 * @param keySpace the keyspace to search
	 * @param columnFamily the group of columns we're considering
	 * @param key the key of the row
	 * @return the key value pairs in the row key or null
	 * @throws StorageClientException
	 */
    Map<String, Object> get(String cacheName, String columnFamily, String key)
        throws StorageClientException;

    /**
     * Insert or update a row in the store.
     * @param keySpace the keyspace to search
     * @param columnFamily the group of columns we're considering
     * @param key the key of the row
     * @param values the Map of column values to associate with this key
     * @param probablyNew whether or not the row is probably new
     * @throws StorageClientException
     */
    void insert(String cacheName, String columnFamily, String key, Map<String, Object> values,
        boolean probablyNew) throws StorageClientException;

    /**
     * Remove a row in the store.
     * @param keySpace the keyspace to search
     * @param columnFamily the group of columns we're considering
     * @param key the key of the row
     * @throws StorageClientException
     */
    void remove(String cacheName, String columnFamily, String key) throws StorageClientException;

    /**
     * Search for indexed content.
     * 
     * @param query The lucene query with which to search the index cache.
     * @return an iterator of results
     * @throws StorageClientException
     */
    QueryIterator find(Query query, Sort sort) throws StorageClientException;

    /**
     * Find the <b>maximum</b> number of results that could be returned in a query. This ignores
     * access control, therefore this number may be significantly different than the number of
     * results returned when executing {@link #find(Query)}.
     * 
     * @param query
     * @return
     * @throws StorageClientException
     */
    int count(Query query) throws StorageClientException;
    
    /**
     * Close this client.
     */
    void close();

    /**
     * List all objects of the type
     * @param keySpace the key space
     * @param columnFamily
     * @return a Disposable iterator containing all raw objects of the type in question.
     * @throws StorageClientException 
     */
    DisposableIterator<SparseRow> listAll(String cacheName) throws StorageClientException;

    /**
     * Count all the objects in a column Family.
     * @param keySpace
     * @param columnFamily
     * @return the number of objects
     * @throws StorageClientException 
     */
    long allCount(String cacheName) throws StorageClientException;
    
    /**
     * Index the list of index documents.
     * 
     * @param keySpace
     * @param columnFamily
     * @param documents
     * @throws StorageClientException
     */
    void updateIndex(String key, Map<String, Object> values) throws StorageClientException;
    
    /**
     * Remove the given documents from the index.
     * 
     * @param documents
     * @throws StorageClientException
     */
    void removeIndex(String key, Map<String, Object> values) throws StorageClientException;
    
    void setStorageClientListener(StorageClientListener storageClientListener);

}
