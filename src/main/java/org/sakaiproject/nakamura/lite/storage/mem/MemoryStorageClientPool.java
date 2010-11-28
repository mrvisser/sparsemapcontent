package org.sakaiproject.nakamura.lite.storage.mem;

import org.apache.commons.pool.BasePoolableObjectFactory;
import org.apache.commons.pool.PoolableObjectFactory;
import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Service;
import org.sakaiproject.nakamura.lite.accesscontrol.CacheHolder;
import org.sakaiproject.nakamura.lite.storage.AbstractClientConnectionPool;
import org.sakaiproject.nakamura.lite.storage.StorageClientPool;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component(enabled = false, metatype = true, inherit = true)
@Service(value = StorageClientPool.class)
public class MemoryStorageClientPool extends AbstractClientConnectionPool {

    public static class ClientConnectionPoolFactory extends BasePoolableObjectFactory {

        private Map<String, Map<String, Object>> store;
        private Map<String, Object> properties;
        private MemoryStorageClientPool pool;

        public ClientConnectionPoolFactory(MemoryStorageClientPool pool,
                Map<String, Map<String, Object>> store, Map<String, Object> properties) {
            this.store = store;
            this.pool = pool;
            this.properties = properties;
        }

        @Override
        public Object makeObject() throws Exception {
            MemoryStorageClient client = new MemoryStorageClient(pool, store, properties);
            return client;
        }

        @Override
        public void passivateObject(Object obj) throws Exception {
            super.passivateObject(obj);
        }

        @Override
        public void activateObject(Object obj) throws Exception {
            super.activateObject(obj);
        }

        @Override
        public void destroyObject(Object obj) throws Exception {
            MemoryStorageClient client = (MemoryStorageClient) obj;
            client.destroy();
        }

        @Override
        public boolean validateObject(Object obj) {
            return super.validateObject(obj);
        }

    }

    private Map<String, Map<String, Object>> store;
    private Map<String, Object> properties;

    public MemoryStorageClientPool() {
    }

    @Activate
    public void activate(Map<String, Object> properties) throws ClassNotFoundException {
        this.properties = properties;
        store = new ConcurrentHashMap<String, Map<String, Object>>();
        super.activate(properties);
    }

    @Deactivate
    public void deactivate(Map<String, Object> properties) {
        super.deactivate(properties);
        store = null;
    }

    @Override
    protected PoolableObjectFactory getConnectionPoolFactory() {
        return new ClientConnectionPoolFactory(this, store, properties);
    }

    @Override
    public Map<String, CacheHolder> getSharedCache() {
        // no point in having a L2 cache for memory.
        return null;
    }

}