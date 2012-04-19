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
package org.sakaiproject.nakamura.lite;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import org.apache.commons.lang.StringUtils;
import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.Service;
import org.sakaiproject.nakamura.api.lite.Configuration;
import org.sakaiproject.nakamura.api.lite.StorageClientUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

@Component(immediate = true, metatype = true)
@Service(value = Configuration.class)
public class ConfigurationImpl implements Configuration {

    @Property(value = "ac")
    protected static final String ACL_COLUMN_FAMILY = "acl-column-family";
    @Property(value = "n")
    protected static final String KEYSPACE = "keyspace";
    @Property(value = "au")
    protected static final String AUTHORIZABLE_COLUMN_FAMILY = "authorizable-column-family";
    @Property(value = "cn")
    protected static final String CONTENT_COLUMN_FAMILY = "content-column-family";
    @Property(value = "lk")
    protected static final String LOCK_COLUMN_FAMILY = "lock-column-family";
    @Property(value = "idx")
    protected static final String INDEX_COLUMN_FAMILY = "index-column-family";
    
    private static final String SHAREDCONFIGPATH = "org/sakaiproject/nakamura/lite/shared.properties";

    protected static final String SHAREDCONFIGPROPERTY = "sparseconfig";
    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigurationImpl.class);
    

    private String aclColumnFamily;
    private String keySpace;
    private String authorizableColumnFamily;
    private String contentColumnFamily;
    private String lockColumnFamily;
    private String indexColumnFamily;
    private Map<String, String> sharedProperties;

    @SuppressWarnings("unchecked")
    @Activate
    public void activate(Map<String, Object> properties) throws IOException {
        aclColumnFamily = StorageClientUtils.getSetting(properties.get(ACL_COLUMN_FAMILY), "ac");
        keySpace = StorageClientUtils.getSetting(properties.get(KEYSPACE), "n");
        authorizableColumnFamily = StorageClientUtils.getSetting(properties.get(AUTHORIZABLE_COLUMN_FAMILY), "au");
        contentColumnFamily = StorageClientUtils.getSetting(properties.get(CONTENT_COLUMN_FAMILY), "cn");
        lockColumnFamily = StorageClientUtils.getSetting(properties.get(LOCK_COLUMN_FAMILY), "ln");

        // load defaults
        // check the classpath
        sharedProperties = Maps.newHashMap();
        InputStream in = this.getClass().getClassLoader().getResourceAsStream(SHAREDCONFIGPATH);
        if ( in != null ) {
            Properties p = new Properties();
            p.load(in);
            in.close();
            sharedProperties.putAll(Maps.fromProperties(p));
        }
        // Load from a properties file defiend on the command line
        String osSharedConfigPath = System.getProperty(SHAREDCONFIGPROPERTY);
        if ( osSharedConfigPath != null && StringUtils.isNotEmpty(osSharedConfigPath)) {
            File f = new File(osSharedConfigPath);
            if ( f.exists() && f.canRead() ) {
                FileReader fr = new FileReader(f);
                Properties p = new Properties();
                p.load(fr);
                fr.close();
                sharedProperties.putAll(Maps.fromProperties(p));
            } else {
                LOGGER.warn("Unable to read shared config file {} specified by the system property {} ",f.getAbsolutePath(), SHAREDCONFIGPROPERTY);
            }
        }

        // make the shared properties immutable.
        sharedProperties = ImmutableMap.copyOf(sharedProperties);
        
    }

    public String getAclColumnFamily() {
        return aclColumnFamily;
    }

    public String getKeySpace() {
        return keySpace;
    }

    public String getAuthorizableColumnFamily() {
        return authorizableColumnFamily;
    }

    public String getContentColumnFamily() {
        return contentColumnFamily;
    }
    
    public String getLockColumnFamily() {
        return lockColumnFamily;
    }
    
    public Map<String, String> getSharedConfig() {
        return sharedProperties;
    }

    public String getIndexColumnFamily() {
      return indexColumnFamily;
    }

}
