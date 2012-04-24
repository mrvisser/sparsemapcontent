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
    @Property(value = "au")
    protected static final String AUTHORIZABLE_COLUMN_FAMILY = "authorizable-column-family";

    protected static final String AUTH_CACHE_NAME = "AuthCache";
    
    protected static final String CONTENT_BODY_CACHE_NAME = "ContentBodyCache";

    protected static final String CONTENT_METADATA_CACHE_NAME = "ContentMetadataCache";

    protected static final String INDEX_CACHE_NAME = "IndexCache";
    
    protected static final String INDEX_DIRECTORY_CACHE_NAME = "IndexCacheDirectory";
    
    private static final String SHAREDCONFIGPATH = "org/sakaiproject/nakamura/lite/shared.properties";

    protected static final String SHAREDCONFIGPROPERTY = "sparseconfig";
    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigurationImpl.class);

    private String aclColumnFamily;
    private String authorizableColumnFamily;
    private Map<String, String> sharedProperties;

    @Activate
    public void activate(Map<String, Object> properties) throws IOException {
        aclColumnFamily = StorageClientUtils.getSetting(properties.get(ACL_COLUMN_FAMILY), "ac");
        authorizableColumnFamily = StorageClientUtils.getSetting(properties.get(AUTHORIZABLE_COLUMN_FAMILY), "au");
        
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

    public String getAuthorizableColumnFamily() {
        return authorizableColumnFamily;
    }

    public Map<String, String> getSharedConfig() {
        return sharedProperties;
    }

    public String getAuthCacheName() {
      return AUTH_CACHE_NAME;
    }

    public String getContentBodyCacheName() {
      return CONTENT_BODY_CACHE_NAME;
    }

    public String getContentMetadataName() {
      return CONTENT_METADATA_CACHE_NAME;
    }

    public String getIndexCacheName() {
      return INDEX_CACHE_NAME;
    }

    public String getIndexStorageCacheName() {
      return INDEX_DIRECTORY_CACHE_NAME;
    }
}
