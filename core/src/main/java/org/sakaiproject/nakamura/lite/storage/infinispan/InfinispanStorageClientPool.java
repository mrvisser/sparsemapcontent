/**
 * 
 */
package org.sakaiproject.nakamura.lite.storage.infinispan;

import org.apache.commons.pool.BasePoolableObjectFactory;
import org.apache.commons.pool.PoolableObjectFactory;
import org.apache.felix.scr.annotations.Reference;
import org.infinispan.Cache;
import org.infinispan.manager.CacheContainer;
import org.sakaiproject.nakamura.api.lite.Configuration;
import org.sakaiproject.nakamura.api.lite.IndexDocument;
import org.sakaiproject.nakamura.api.lite.IndexDocumentFactory;
import org.sakaiproject.nakamura.api.lite.StorageCacheManager;
import org.sakaiproject.nakamura.lite.storage.spi.AbstractClientConnectionPool;

import java.util.List;

/**
 * @author Branden
 *
 */
public class InfinispanStorageClientPool extends AbstractClientConnectionPool {

	private final CacheContainer cacheContainer;
	private final PoolableObjectFactory factory = new ClientConnectionPoolFactory();
	
	protected List<IndexDocumentFactory> indexes;
	
	@Reference
	protected Configuration configuration;
	
	public InfinispanStorageClientPool(CacheContainer cacheContainer, Configuration configuration,
	    List<IndexDocumentFactory> indexes) {
		this.cacheContainer = cacheContainer;
		this.configuration = configuration;
		this.indexes = indexes;
	}
	
	public StorageCacheManager getStorageCacheManager() {
		return null;
	}

	@Override
	protected PoolableObjectFactory getConnectionPoolFactory() {
		return factory;
	}

	public class ClientConnectionPoolFactory extends BasePoolableObjectFactory {
		@Override
		public Object makeObject() throws Exception {
		  Cache<String, IndexDocument> indexCache = cacheContainer.getCache(configuration.getIndexCacheName());
			return new InfinispanStorageClient(cacheContainer, indexCache, indexes);
		}
	}
}
