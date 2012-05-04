/**
 * 
 */
package org.sakaiproject.nakamura.lite.storage.infinispan;

import com.google.common.collect.Maps;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.infinispan.Cache;
import org.infinispan.manager.CacheContainer;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.QueryIterator;
import org.infinispan.query.SearchManager;
import org.sakaiproject.nakamura.api.lite.IndexDocument;
import org.sakaiproject.nakamura.api.lite.IndexDocumentFactory;
import org.sakaiproject.nakamura.api.lite.RemoveProperty;
import org.sakaiproject.nakamura.api.lite.StorageClientException;
import org.sakaiproject.nakamura.api.lite.content.Content;
import org.sakaiproject.nakamura.lite.ClassLoaderHelper;
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

	private static final Logger LOGGER = LoggerFactory.getLogger(InfinispanStorageClient.class);

	private final CacheContainer storageContainer;
	private final Cache<String, IndexDocument> indexCache;
	private final List<IndexDocumentFactory> indexes;
	private StorageClientListener storageClientListener;

	public InfinispanStorageClient(CacheContainer storageContainer,
	    Cache<String, IndexDocument> indexCache, List<IndexDocumentFactory> indexes) {
		this.storageContainer = storageContainer;
		this.indexCache = indexCache;
		this.indexes = indexes;
	}

	public Map<String, Object> get(String cacheName, String columnFamily, String key)
	    throws StorageClientException {
		return getMapFromStorage(cacheName, getNamespacedKey(columnFamily, key));
	}

	public void insert(String cacheName, String columnFamily, String key,
			Map<String, Object> values, boolean probablyNew)
			throws StorageClientException {
	  String nsKey = getNamespacedKey(columnFamily, key);
		Map<String, Object> before = get(cacheName, columnFamily, key);
		Map<String, Object> mutableValues = new HashMap<String, Object>();
		if (before != null) {
		  mutableValues.putAll(before);
		}

		for (Map.Entry<String, Object> entry : values.entrySet()) {
			String columnName = entry.getKey();
			if (entry.getValue() instanceof RemoveProperty) {
				mutableValues.remove(columnName);
			} else if (entry.getValue() == null) {
			  mutableValues.remove(columnName);
			} else {
				mutableValues.put(columnName, entry.getValue());
			}
		}
		
		updateIndex(key, mutableValues);

		if (storageClientListener != null) {
			storageClientListener.before(cacheName, key, before);
		}
		
		getCache(cacheName).put(nsKey, mutableValues);

		if (storageClientListener != null) {
			storageClientListener.after(cacheName, key,
					mutableValues);
		}
	}

	public void remove(String cacheName, String columnFamily, String key)
			throws StorageClientException {
	  String nsKey = getNamespacedKey(columnFamily, key);
		Map<String, Object> values = get(cacheName, columnFamily, key);
		if (values != null) {
		  removeIndex(key, values);
			getCache(cacheName).remove(nsKey);
		}
	}

  public QueryIterator find(Query query, Sort sort) throws StorageClientException {
	  return getCacheQuery(query, sort).lazyIterator();
	}

  public int count(Query query) throws StorageClientException {
    return getCacheQuery(query, null).getResultSize();
  }
  
  private CacheQuery getCacheQuery(Query query, Sort sort) throws StorageClientException {
    SearchManager searchManager = org.infinispan.query.Search.getSearchManager(indexCache);
    CacheQuery cacheQuery = searchManager.getQuery(query);
    if (sort != null) {
      cacheQuery.sort(sort);
    }
    return cacheQuery;
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
	
	public void removeIndex(String key, Map<String, Object> values) throws StorageClientException {
	  List<IndexDocument> documents = getIndexedDocuments(key, values);
	  if (documents != null) {
	    ClassLoader prev = ClassLoaderHelper.swapContext(getClass().getClassLoader());
	    try {
  	    for (IndexDocument document : documents) {
  	      String docKey = getDocKey(document);
  	      indexCache.remove(docKey);
  	    }
	    } finally {
	      ClassLoaderHelper.swapContext(prev);
	    }
	  }
	}

  public void updateIndex(String key, Map<String, Object> values) throws StorageClientException {
    List<IndexDocument> documents = getIndexedDocuments(key, values);
    if (documents != null) {
      // we need to set the bundle class loader to the thread context, to work with JNDI/Hibernate
      ClassLoader prev = ClassLoaderHelper.swapContext(getClass().getClassLoader());
      try {
        for (IndexDocument document : documents) {
          // namespace the key by the document class to avoid collisions with other indexed documents
          indexCache.put(getDocKey(document), document);
        }
      } finally {
        ClassLoaderHelper.swapContext(prev);
      }
    }
  }
  
  private String getDocKey(IndexDocument document) {
    return String.format("%s:%s", document.getClass().getCanonicalName(),
        document.getId());
  }
  
	private List<IndexDocument> getIndexedDocuments(String key, Map<String, Object> content) {
	  List<IndexDocument> toIndex = new LinkedList<IndexDocument>();
    for (IndexDocumentFactory factory : indexes) {
      IndexDocument doc = factory.createIndexDocument(key, content);
      if (doc != null) {
        doc.setId(key);
        toIndex.add(doc);
      }
    }
    return toIndex;
	}
	
	private String getNamespacedKey(String columnFamily, String key) {
	  return String.format("%s:%s", columnFamily, key);
	}
	
	@SuppressWarnings("unchecked")
	private Map<String, Object> getMapFromStorage(String cacheName, String key) throws StorageClientException {
	  Map<String, Object> result = Maps.newHashMap();
	  Map<String, Object> stored = (Map<String, Object>) getCache(cacheName).get(key); 
		if (stored != null) {
		  result.putAll(stored);
		}
		return result;
	}

	private <K, V> Cache<K, V> getCache(String cacheName) throws StorageClientException {
		Cache<K, V> cache = storageContainer.getCache(cacheName);
		if (cache == null) {
			throw new StorageClientException(String.format("Failed to obtain cache '%s'", cacheName));
		}
		return cache;
	}
	
	public void addIndexDocumentFactory(IndexDocumentFactory factory) {
	  this.indexes.add(factory);
	}
	
	public void removeIndexDocumentFactory(IndexDocumentFactory factory) {
	  this.indexes.remove(factory);
	}
}
