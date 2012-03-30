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
package org.sakaiproject.nakamura.lite.storage.jdbc;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Enumeration;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Timer;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.pool.PoolableObjectFactory;
import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.Reference;
import org.sakaiproject.nakamura.api.lite.ClientPoolException;
import org.sakaiproject.nakamura.api.lite.Configuration;
import org.sakaiproject.nakamura.api.lite.StorageCacheManager;
import org.sakaiproject.nakamura.api.lite.StorageClientException;
import org.sakaiproject.nakamura.api.lite.StorageClientUtils;
import org.sakaiproject.nakamura.lite.storage.spi.AbstractClientConnectionPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;

import edu.umd.cs.findbugs.annotations.SuppressWarnings;

/**
 * An base class for JDBC drivers. If you change the OSGi configuration of this
 * class you will need to re-build all fragment bundles that contain code extending this.
 * @author ieb
 *
 */
@Component(componentAbstract = true)
public class BaseJDBCStorageClientPool extends AbstractClientConnectionPool {

    private static final Logger LOGGER = LoggerFactory.getLogger(BaseJDBCStorageClientPool.class);

    @Property(value = { "jdbc:derby:sling/sparsemap/db;create=true" })
    public static final String CONNECTION_URL = "jdbc-url";
    @Property(value = { "org.apache.derby.jdbc.EmbeddedDriver" })
    public static final String JDBC_DRIVER = "jdbc-driver";

    @Property(value = { "sa" })
    public static final String USERNAME = "username";
    @Property(value = { "" })
    public static final String PASSWORD = "password";
 
    /**
     * Clients should provide an implementation of NamedCacheManager in preference to this cache manager. 
     */
    @Reference
    private StorageCacheManager storageManagerCache;



    private static final String BASESQLPATH = "org/sakaiproject/nakamura/lite/storage/jdbc/config/client";

    public class JCBCStorageClientConnection implements PoolableObjectFactory {

        public JCBCStorageClientConnection() {
        }

        public void activateObject(Object obj) throws Exception {
            JDBCStorageClient client = checkSchema(obj);
            client.activate();
        }

        public void destroyObject(Object obj) throws Exception {
            JDBCStorageClient client = (JDBCStorageClient) obj;
            client.destroy();
        }

        public Object makeObject() throws Exception {
            return checkSchema(new JDBCStorageClient(BaseJDBCStorageClientPool.this, properties,
                    getSqlConfig(), getIndexColumns(), getIndexColumnsTypes(), getIndexColumnsNames(), true ));
        }

        public void passivateObject(Object obj) throws Exception {
            JDBCStorageClient client = (JDBCStorageClient) obj;
            client.passivate();
        }

        public boolean validateObject(Object obj) {
            JDBCStorageClient client = checkSchema(obj);
            try {
                return client.validate();
            } catch (StorageClientException e) {
                return false;
            }
        }

    }

    private Map<String, Object> properties;
    private boolean schemaHasBeenChecked = false;
    private Map<String, Object> sqlConfig;
    private Object sqlConfigLock = new Object();

    private Properties connectionProperties;

    private String username;

    private String password;

    private String url;

    private ConnectionManager connectionManager;

    private Timer timer;

    private Map<String, String> indexColumnsMap;

    @Override
    @Activate
    @SuppressWarnings(value={"NP_CLOSING_NULL"},justification="Invalid report, if this was the case then nothing would work")
    public void activate(Map<String, Object> properties) throws ClassNotFoundException {
        this.properties = properties;
        super.activate(properties);

        connectionManager = new ConnectionManager(this);
        timer = new Timer();
        timer.schedule(connectionManager, 30000L, 30000L);

        // this is a default cache used where none has been provided.
        if ( LOGGER.isDebugEnabled()) {
            DriverManager.setLogWriter(new PrintWriter(System.err));
        }

        String jdbcDriver = StorageClientUtils.getSetting(properties.get(JDBC_DRIVER),"");
        Class<?> driverClass = this.getClass().getClassLoader().loadClass(jdbcDriver);
        if ( driverClass != null  ) {
            LOGGER.info("Loaded Driver Class {} with classloader {} ", driverClass, driverClass.getClassLoader());
            try {
                Driver d = (Driver) driverClass.newInstance();
                LOGGER.info("Created Driver Instance as {} ", d);
            } catch (InstantiationException e) {
                LOGGER.info("Error Creating Driver {} ", driverClass, e);
            } catch (IllegalAccessException e) {
                LOGGER.info("Error Creating Driver {} ", driverClass, e);
            }
        } else {
            LOGGER.error("Failed to Load the DB Driver {}, unless the driver is available in the core bundle, it probably wont be found.", jdbcDriver);
        }
        connectionProperties = getConnectionProperties(properties);
        username = StorageClientUtils.getSetting(properties.get(USERNAME), "");
        password = StorageClientUtils.getSetting(properties.get(PASSWORD), "");
        url = StorageClientUtils.getSetting(properties.get(CONNECTION_URL), "");

        LOGGER.info("Loaded Database Driver {} as {}  ", jdbcDriver, driverClass);
        boolean registered = false;
        for ( Enumeration<Driver> ed = DriverManager.getDrivers(); ed.hasMoreElements();) {
            registered = true;
            Driver d = ed.nextElement();
            LOGGER.info("JDBC Driver Registration [{}] [{}] [{}] ", new Object[]{d, d.getClass(), d.getClass().getClassLoader()});
        }
        if ( !registered ) {
            LOGGER.warn("The SQL Driver has no drivers registered, did you ensure that that your Driver started up before this bundle ?");
        }
        JDBCStorageClient client = null;
        try {
            // dont use the pool, we dont want this client to be in the pool.
            client = new JDBCStorageClient(this, properties,
                    getSqlConfig(), getIndexColumns(), getIndexColumnsTypes(), getIndexColumnsNames(), false );
            client = checkSchema(client);
            if (client == null) {
                LOGGER.warn("Failed to check Schema, no connection");
            }
        } catch (ClientPoolException e) {
            LOGGER.warn("Failed to check Schema", e);
        } catch (NoSuchAlgorithmException e) {
            LOGGER.warn("Failed to check Schema", e);
        } catch (SQLException e) {
            LOGGER.warn("Failed to check Schema", e);
        } catch (StorageClientException e) {
            LOGGER.warn("Failed to check Schema", e);
        } finally {
          if (client != null) {
              // do not close as this will add the client into the pool.
            client.passivate();
            client.destroy();
          }
        }

    }




    public Map<String, String> getIndexColumnsNames() {
        return indexColumnsMap;
    }


    @Override
    @Deactivate
    public void deactivate(Map<String, Object> properties) {
        super.deactivate(properties);

        timer.cancel();
        connectionManager.close();

        String connectionUrl = StorageClientUtils.getSetting(this.properties.get(CONNECTION_URL),"");
        String jdbcDriver = StorageClientUtils.getSetting(properties.get(JDBC_DRIVER),"");
        if ("org.apache.derby.jdbc.EmbeddedDriver".equals(jdbcDriver) && connectionUrl != null) {
            // need to shutdown this instance.
            String[] parts = StringUtils.split(connectionUrl, ';');
            Connection connection = null;
            try {
                connection = DriverManager.getConnection(parts[0] + ";shutdown=true");
            } catch (SQLException e) {
                // yes really see
                // http://db.apache.org/derby/manuals/develop/develop15.html#HDRSII-DEVELOP-40464
                LOGGER.info("Sparse Map Content Derby Embedded instance shutdown sucessfully {}",
                        e.getMessage());
            } finally {
                if (connection != null) {
                    try {
                        connection.close();
                    } catch (SQLException e) {
                        LOGGER.debug(
                                "Very Odd, the getConnection should not have opened a connection (see DerbyDocs),"
                                        + " but it did, and when we tried to close it we got  "
                                        + e.getMessage(), e);
                    }
                }
            }
        }
    }

    protected JDBCStorageClient checkSchema(Object o) {
        JDBCStorageClient client = (JDBCStorageClient) o;
        synchronized (sqlConfigLock) {
            if (!schemaHasBeenChecked) {
                try {
                    Connection connection = client.getConnection();
                    DatabaseMetaData metadata = connection.getMetaData();
                    LOGGER.info("Starting Sparse Map Content database ");
                    LOGGER.info("   Database Vendor: {} {}", metadata.getDatabaseProductName(),
                            metadata.getDatabaseProductVersion());
                    LOGGER.info("   Database Driver: {} ", properties.get(JDBC_DRIVER));
                    LOGGER.info("   Database URL   : {} ", properties.get(CONNECTION_URL));
                    client.checkSchema(getClientConfigLocations(client.getConnection()));
                    schemaHasBeenChecked = true;
                    indexColumnsMap = client.syncIndexColumns();
                } catch (Throwable e) {
                    LOGGER.warn("Failed to check Schema", e);
                }
            }
        }
        return client;
    }

    public Map<String, Object> getSqlConfig() {
        return getSqlConfig(null);
    }

    public Map<String, Object> getSqlConfig(Connection connection) {
        synchronized (sqlConfigLock) {
            if (sqlConfig == null) {
                try {
                    if ( connection == null ) {
                        connection = getConnection();
                    }
                    for (String clientSQLLocation : getClientConfigLocations(connection)) {
                        String clientConfig = clientSQLLocation + ".sql";
                        InputStream in = this.getClass().getClassLoader()
                                .getResourceAsStream(clientConfig);
                        if (in != null) {
                            try {
                                Properties p = new Properties();
                                p.load(in);
                                in.close();
                                Builder<String, Object> b = ImmutableMap.builder();
                                for (Entry<Object, Object> e : p.entrySet()) {
                                    b.put(String.valueOf(e.getKey()), e.getValue());
                                }
                                sqlConfig = b.build();
                                LOGGER.info("Using SQL configuation from {} ", clientConfig);
                                break;
                            } catch (IOException e) {
                                LOGGER.info("Failed to read {} ", clientConfig, e);
                            }
                        } else {
                            LOGGER.info("No SQL configuation at {} ", clientConfig);
                        }
                    }
                } catch (SQLException e) {
                    LOGGER.error("Failed to locate SQL configuration ",e);
                }
            }
        }
        return sqlConfig;
    }

    private String[] getClientConfigLocations(Connection connection) throws SQLException {
        String dbProductName = connection.getMetaData().getDatabaseProductName()
                .replaceAll(" ", "");
        int dbProductMajorVersion = connection.getMetaData().getDatabaseMajorVersion();
        int dbProductMinorVersion = connection.getMetaData().getDatabaseMinorVersion();

        return new String[] {
                BASESQLPATH + "." + dbProductName + "." + dbProductMajorVersion + "."
                        + dbProductMinorVersion,
                BASESQLPATH + "." + dbProductName + "." + dbProductMajorVersion,
                BASESQLPATH + "." + dbProductName, BASESQLPATH };
    }

    private Properties getConnectionProperties(Map<String, Object> config) {
        Properties connectionProperties = new Properties();
        for (Entry<String, Object> e : config.entrySet()) {
            // dont add the configuration object that might be in the properties while unit testing.
            if ( !(e.getValue() instanceof Configuration) ) {
                connectionProperties.put(e.getKey(), e.getValue());
            }
        }
        return connectionProperties;
    }

    @Override
    protected PoolableObjectFactory getConnectionPoolFactory() {
        return new JCBCStorageClientConnection();
    }

    public StorageCacheManager getStorageCacheManager() {
        return storageManagerCache;
    }

    public Connection getConnection() throws SQLException {
        Connection connection = connectionManager.get();
        if (connection == null) {
            if ("".equals(username)) {
                connection = DriverManager.getConnection(url, connectionProperties);
            } else {
                connection = DriverManager.getConnection(url, username, password);
            }
            connection.setAutoCommit(true); // KERN-1691
            connectionManager.set(connection);
        }
        return connection;
    }




    public String getValidationSql() {
        if ( sqlConfig != null ) {
            return (String) sqlConfig.get("validate");
        }
        return null;
    }

}
