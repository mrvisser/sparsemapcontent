/**
 * 
 */
package org.sakaiproject.nakamura.lite.storage.infinispan;

import org.apache.commons.pool.BasePoolableObjectFactory;
import org.apache.commons.pool.PoolableObjectFactory;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Service;
import org.infinispan.Cache;
import org.infinispan.manager.DefaultCacheManager;
import org.sakaiproject.nakamura.api.lite.StorageCacheManager;
import org.sakaiproject.nakamura.lite.storage.spi.AbstractClientConnectionPool;
import org.sakaiproject.nakamura.lite.storage.spi.StorageClientPool;

/**
 * @author Branden
 *
 */
@Component(enabled = true, metatype = true, inherit = true)
@Service(value = StorageClientPool.class)
public class InfinispanStorageClientPool extends AbstractClientConnectionPool {

	private final Cache<String, Object> storage;
	private final PoolableObjectFactory factory = new ClientConnectionPoolFactory();
	
	public InfinispanStorageClientPool() {
		storage = (new DefaultCacheManager(true)).getCache();	
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
			return new InfinispanStorageClient(storage, getIndexColumns());
		}
	}
}
