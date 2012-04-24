/**
 * 
 */
package org.sakaiproject.nakamura.lite.storage.infinispan;

import org.infinispan.Cache;
import org.infinispan.manager.CacheContainer;
import org.sakaiproject.nakamura.api.lite.ClientPoolException;
import org.sakaiproject.nakamura.api.lite.Configuration;
import org.sakaiproject.nakamura.api.lite.IndexDocument;
import org.sakaiproject.nakamura.api.lite.IndexDocumentFactory;
import org.sakaiproject.nakamura.api.lite.StorageCacheManager;
import org.sakaiproject.nakamura.lite.storage.spi.StorageClient;
import org.sakaiproject.nakamura.lite.storage.spi.StorageClientPool;

import java.util.List;

/**
 * @author Branden
 *
 */
public class InfinispanStorageClientPool implements StorageClientPool {

	private final StorageClient client;
	
	public InfinispanStorageClientPool(CacheContainer cacheContainer, Configuration configuration,
	    List<IndexDocumentFactory> indexes) {
	  Cache<String, IndexDocument> indexCache = cacheContainer.getCache(
	      configuration.getIndexCacheName());
		this.client = new InfinispanStorageClient(cacheContainer, indexCache, indexes);
	}
	
	public StorageCacheManager getStorageCacheManager() {
		return null;
	}

  public StorageClient getClient() throws ClientPoolException {
    return client;
  }

}
