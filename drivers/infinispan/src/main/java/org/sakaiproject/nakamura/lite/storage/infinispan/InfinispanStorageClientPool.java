/**
 * 
 */
package org.sakaiproject.nakamura.lite.storage.infinispan;

import org.apache.commons.pool.BasePoolableObjectFactory;
import org.apache.commons.pool.PoolableObjectFactory;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.Service;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.DefaultCacheManager;
import org.sakaiproject.nakamura.api.lite.Configuration;
import org.sakaiproject.nakamura.api.lite.IndexDocument;
import org.sakaiproject.nakamura.api.lite.IndexDocumentFactory;
import org.sakaiproject.nakamura.api.lite.StorageCacheManager;
import org.sakaiproject.nakamura.lite.storage.spi.AbstractClientConnectionPool;
import org.sakaiproject.nakamura.lite.storage.spi.StorageClientPool;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * @author Branden
 *
 */
@Component(enabled = true, metatype = true, inherit = true)
@Service(value = StorageClientPool.class)
public class InfinispanStorageClientPool extends AbstractClientConnectionPool {

	private final CacheContainer cacheContainer;
	private final PoolableObjectFactory factory = new ClientConnectionPoolFactory();
	
	protected Map<String, List<IndexDocumentFactory>> cacheIndexes = 
	    new HashMap<String, List<IndexDocumentFactory>>();
	
	@Reference
	protected Configuration configuration;
	
	@Reference
	protected CopyOnWriteArrayList<IndexDocument> contentIndexDocuments;
	
	
	public InfinispanStorageClientPool() {
		cacheContainer = new DefaultCacheManager(true);
		// TODO: Configure the cache container somehow.
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
			return new InfinispanStorageClient(cacheContainer, configuration.getIndexColumnFamily(),
			    cacheIndexes);
		}
	}
}
