/**
 * 
 */
package org.sakaiproject.nakamura.lite.storage.infinispan;

import org.apache.lucene.search.Filter;
import org.hibernate.search.query.dsl.QueryBuilder;
import org.infinispan.Cache;
import org.infinispan.manager.CacheContainer;
import org.infinispan.query.SearchManager;
import org.sakaiproject.nakamura.api.lite.IndexDocument;
import org.sakaiproject.nakamura.api.lite.IndexDocumentFactory;
import org.sakaiproject.nakamura.api.lite.RemoveProperty;
import org.sakaiproject.nakamura.api.lite.StorageClientException;
import org.sakaiproject.nakamura.api.lite.StorageClientUtils;
import org.sakaiproject.nakamura.api.lite.content.Content;
import org.sakaiproject.nakamura.lite.storage.spi.DirectCacheAccess;
import org.sakaiproject.nakamura.lite.storage.spi.DisposableIterator;
import org.sakaiproject.nakamura.lite.storage.spi.Disposer;
import org.sakaiproject.nakamura.lite.storage.spi.SparseMapRow;
import org.sakaiproject.nakamura.lite.storage.spi.SparseRow;
import org.sakaiproject.nakamura.lite.storage.spi.StorageClient;
import org.sakaiproject.nakamura.lite.storage.spi.StorageClientListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * @author Branden
 */
public class InfinispanStorageClient implements StorageClient {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(InfinispanStorageClient.class);

	private final CacheContainer storageContainer;
	private final String indexCacheName;
	private final Map<String, List<IndexDocumentFactory>> cacheIndexes;
	private StorageClientListener storageClientListener;

	public InfinispanStorageClient(CacheContainer storageContainer, String indexCacheName,
	    Map<String, List<IndexDocumentFactory>> cacheIndexes) {
		this.storageContainer = storageContainer;
		this.indexCacheName = indexCacheName;
		this.cacheIndexes = cacheIndexes;
	}

	public Map<String, Object> get(String cacheName, String key) throws StorageClientException {
		return getMapFromStorage(cacheName, key);
	}

	public void insert(String cacheName, String key,
			Map<String, Object> values, boolean probablyNew)
			throws StorageClientException {
		Map<String, Object> before = get(cacheName, key);
		Map<String, Object> mutableValues = new HashMap<String, Object>(before);

		for (Map.Entry<String, Object> entry : values.entrySet()) {
			String columnName = entry.getKey();
			if (entry.getValue() instanceof RemoveProperty) {
				mutableValues.remove(columnName);
			} else {
				mutableValues.put(columnName, entry.getValue());
			}
		}
		
		index(cacheName, key, mutableValues);

		if (storageClientListener != null) {
			storageClientListener.before(cacheName, key, before);
		}
		
		getCache(cacheName).put(key, mutableValues);

		if (storageClientListener != null) {
			storageClientListener.after(cacheName, key,
					mutableValues);
		}
	}

	public void remove(String cacheName, String key)
			throws StorageClientException {
		Map<String, Object> values = get(cacheName, key);
		if (values != null) {
		  List<IndexDocument> documents = getIndexedDocuments(cacheName, key, values);
		  removeIndex(documents);
			getCache(cacheName).remove(key);
		}
	}

	public DisposableIterator<Map<String, Object>> find(Class<? extends IndexDocument> clazz,
	    Map<String, Object> properties, DirectCacheAccess cachingManager)
	    throws StorageClientException {
	  SearchManager searchManager = org.infinispan.query.Search.getSearchManager(
	      getCache(indexCacheName));
	  QueryBuilder queryBuilder = searchManager.buildQueryBuilderForClass(clazz).get();
	  for (Map.Entry<String, Object> entry : properties.entrySet()) {
	    String k = entry.getKey();
	    Object v = entry.getValue();
	    if (v != null) {
  	    if (v instanceof Map) {
  	      for (Map.Entry<String, Object> subEntry : ((Map<String, Object>)v).entrySet()) {
  	        String subk = subEntry.getKey();
  	        Object subv = subEntry.getValue();
  	        
  	      }
  	    }
	    }
	  }
	  return null;
	}

	public void close() {
	}

	public DisposableIterator<SparseRow> listAll(String cacheName) throws StorageClientException {
		final Iterator<Entry<Object, Object>> i = getCache(cacheName).entrySet().iterator(); 
		return new DisposableIterator<SparseRow>() {

			public boolean hasNext() {
				return i.hasNext();
			}

			public SparseRow next() {
				Entry<Object, Object> entry = i.next();
				@SuppressWarnings("unchecked")
				Map<String, Object> content = (Map<String, Object>) entry.getValue();
				return new SparseMapRow((String)content.get(Content.UUID_FIELD), content);
			}

			public void remove() {
				i.remove();
			}

			public void close() { }
			public void setDisposer(Disposer disposer) { }
			
		};
	}

	public long allCount(String cacheName)
			throws StorageClientException {
		return getCache(cacheName).entrySet().size();
	}

	public void setStorageClientListener(StorageClientListener storageClientListener) {
		this.storageClientListener = storageClientListener;
	}
	
	public void updateIndex(List<IndexDocument> documents) throws StorageClientException {
	  Cache<Object, Object> cache = getCache(indexCacheName);
	  if (documents != null) {
	    for (IndexDocument document : documents) {
	      cache.put(StorageClientUtils.getInternalUuid(), document);
	    }
	  }
	}
	
	public void removeIndex(List<IndexDocument> documents) throws StorageClientException {
	  Cache<Object, Object> cache = getCache(indexCacheName);
	  if (documents != null) {
	    for (IndexDocument document : documents) {
	      cache.remove(document.getId());
	    }
	  }
	}
	
	private void index(String sourceCacheName, String key, Map<String, Object> content)
	    throws StorageClientException {
	  updateIndex(getIndexedDocuments(sourceCacheName, key, content));
	}
	
	private List<IndexDocument> getIndexedDocuments(String sourceCacheName, String key,
	    Map<String, Object> content) {
	  List<IndexDocument> toIndex = new LinkedList<IndexDocument>();
	  List<IndexDocumentFactory> factories = cacheIndexes.get(sourceCacheName);
    for (IndexDocumentFactory factory : factories) {
      IndexDocument doc = factory.createIndexDocument(key, content);
      if (doc != null) {
        doc.setId(key);
        toIndex.add(doc);
      }
    }
    return toIndex;
	}
	
	@SuppressWarnings("unchecked")
	private Map<String, Object> getMapFromStorage(String cacheName, String key) throws StorageClientException {
		return (Map<String, Object>) getCache(cacheName).get(key);
	}

	private <K, V> Cache<K, V> getCache(String cacheName) throws StorageClientException {
		Cache<K, V> cache = storageContainer.getCache(cacheName);
		if (cache == null) {
			throw new StorageClientException(String.format("Failed to obtain cache '%s'", cacheName));
		}
		return cache;
	}
	
}
