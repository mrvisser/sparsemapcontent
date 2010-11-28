package org.sakaiproject.nakamura.lite.jdbc.derby;

import com.google.common.collect.ImmutableMap;

import org.sakaiproject.nakamura.lite.storage.jdbc.JDBCStorageClientPool;

public class DerbySetup {

    private static JDBCStorageClientPool clientPool = createClientPool();

    private synchronized static JDBCStorageClientPool createClientPool() {
        try {
            JDBCStorageClientPool connectionPool = new JDBCStorageClientPool();
            connectionPool.activate(ImmutableMap.of(JDBCStorageClientPool.CONNECTION_URL,
                    (Object) "jdbc:derby:memory:MyDB;create=true",
                    JDBCStorageClientPool.JDBC_DRIVER, "org.apache.derby.jdbc.EmbeddedDriver"));
            return connectionPool;
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public static JDBCStorageClientPool getClientPool() {
        return clientPool;
    }

}
