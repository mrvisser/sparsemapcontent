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
package org.sakaiproject.nakamura.lite.content;

import static org.sakaiproject.nakamura.lite.content.InternalContent.COPIED_DEEP_FIELD;
import static org.sakaiproject.nakamura.lite.content.InternalContent.COPIED_FROM_PATH_FIELD;
import static org.sakaiproject.nakamura.lite.content.InternalContent.CREATED_BY_FIELD;
import static org.sakaiproject.nakamura.lite.content.InternalContent.CREATED_FIELD;
import static org.sakaiproject.nakamura.lite.content.InternalContent.DELETED_FIELD;
import static org.sakaiproject.nakamura.lite.content.InternalContent.LASTMODIFIED_BY_FIELD;
import static org.sakaiproject.nakamura.lite.content.InternalContent.LASTMODIFIED_FIELD;
import static org.sakaiproject.nakamura.lite.content.InternalContent.PATH_FIELD;
import static org.sakaiproject.nakamura.lite.content.InternalContent.READONLY_FIELD;
import static org.sakaiproject.nakamura.lite.content.InternalContent.TRUE;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.infinispan.Cache;
import org.infinispan.io.GridFile.Metadata;
import org.infinispan.io.GridFilesystem;
import org.infinispan.manager.CacheContainer;
import org.infinispan.query.QueryIterator;
import org.sakaiproject.nakamura.api.lite.CacheHolder;
import org.sakaiproject.nakamura.api.lite.Configuration;
import org.sakaiproject.nakamura.api.lite.IndexDocument;
import org.sakaiproject.nakamura.api.lite.RemoveProperty;
import org.sakaiproject.nakamura.api.lite.Repository;
import org.sakaiproject.nakamura.api.lite.StorageClientException;
import org.sakaiproject.nakamura.api.lite.StorageClientUtils;
import org.sakaiproject.nakamura.api.lite.StoreListener;
import org.sakaiproject.nakamura.api.lite.accesscontrol.AccessControlManager;
import org.sakaiproject.nakamura.api.lite.accesscontrol.AccessDeniedException;
import org.sakaiproject.nakamura.api.lite.accesscontrol.AclModification;
import org.sakaiproject.nakamura.api.lite.accesscontrol.AclModification.Operation;
import org.sakaiproject.nakamura.api.lite.accesscontrol.Permissions;
import org.sakaiproject.nakamura.api.lite.accesscontrol.PrincipalTokenResolver;
import org.sakaiproject.nakamura.api.lite.accesscontrol.Security;
import org.sakaiproject.nakamura.api.lite.authorizable.User;
import org.sakaiproject.nakamura.api.lite.content.ActionRecord;
import org.sakaiproject.nakamura.api.lite.content.Content;
import org.sakaiproject.nakamura.api.lite.content.ContentManager;
import org.sakaiproject.nakamura.api.lite.util.PreemptiveIterator;
import org.sakaiproject.nakamura.lite.CachingManagerImpl;
import org.sakaiproject.nakamura.lite.content.io.GridFileInputStreamWrapper;
import org.sakaiproject.nakamura.lite.storage.spi.StorageClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * <pre>
 * Content Manager.
 * Manages two types of content,
 * Bundles of content properties and bodies.
 * Bodies are chunked into sizes to aide efficiency when retrieving the content.
 * 
 * CF content stores the structure of the content keyed by path.
 * Each item contains child names in columns + the guid of the item
 * eg
 *   path : {
 *       ':id' : thisitemUUID,
 *       subitemA : subitemAUUID,
 *       subitemB : subitemBUUID
 *   }
 * the guid of the item points to the CF content version where items are keyed by the version.
 * These items also contain child nodes under children as an array
 * 
 * eg
 *    itemUUID : {
 *         'id' : thisitemUUID
 *         'children' : [ 
 *           subitemA : subitemAUUID,
 *           subitemB : subitemBUUID
 *         ],
 *         'nblocks' = numberOfBlocksSetsOfContent
 *         'length' = totalLenghtOftheContent
 *         'blocksize' = storageBlockSize
 *         'blockid' = blockID
 *         ... other properties ...
 *    }
 *    
 * The content blocks are stored in CF content body
 * eg
 *   blockID:blockSetNumber : {
 *         'id' : blockID,
 *         'numblocks' : numberOfBlocksInThisSet,
 *         'blocklength0' : lengthOfThisBlock,
 *         'body0' : byte[]
 *         'blocklength1' : lengthOfThisBlock,
 *         'body1' : byte[]
 *         ...
 *         'blocklengthn' : lengthOfThisBlock,
 *         'bodyn' : byte[]
 *    }
 * 
 * 
 * Versioning:
 * 
 * When a version is saved, the CF contentVersion item is cloned and the CF content :id and any subitems IDs are updated.
 * Block 0 is marked as readonly
 * 
 * When the body is written to its CF content row is checked to see if the block is read only. If so a new block is created with and linked in with 'previousversion'
 * A version object is also created to keep track of the versions.
 * 
 * </pre>
 * 
 * @author ieb
 * 
 */
public class ContentManagerImpl extends CachingManagerImpl implements ContentManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(ContentManagerImpl.class);

    private static final String STREAMS_FIELD = "_streams";
    
    private static final Set<String> PROTECTED_FIELDS = ImmutableSet.of(PATH_FIELD,
      LASTMODIFIED_FIELD, LASTMODIFIED_BY_FIELD, STREAMS_FIELD);

    // These properties copied from AccessControlManager to keep from binding
    // directly to the implementation class. They should stay in sync.
    private static final String _SECRET_KEY = "_secretKey";
    private static final String _PATH = "_aclPath";
    private static final String _OBJECT_TYPE = "_aclType";
    private static final String _KEY = "_aclKey";
    private static final Set<String> ACL_READ_ONLY_PROPERTIES = ImmutableSet.of(_SECRET_KEY, _PATH, _OBJECT_TYPE, _KEY);
    
    private static final String FILESYSTEM_ROOT = "/.oae";
    private static final String PREFIX_CONTENT = "oae";
    private static final String PREFIX_STREAM = String.format("%s:%s", PREFIX_CONTENT, "stream");
    private static final String PREFIX_VERSION = String.format("%s:%s", PREFIX_CONTENT, "version");
    private static final String PREFIX_PROP_VERSION_METADATA = "metadata";
    
    private static final String FILE_CHILDREN = String.format("%s:%s", PREFIX_CONTENT, "children");
    private static final String FILE_PROPERTIES = String.format("%s:%s", PREFIX_CONTENT, "props");
    private static final String FILE_STREAM_DEFAULT = PREFIX_STREAM;

    /**
     * The file meta-data cache that can be used to discover all known content.
     */
    private Cache<String, Metadata> metaCache;

    /**
     * The grid file-system on which content will be stored.
     */
    private GridFilesystem fs;
    
    /**
     * A helper class for performing bulk file-system operations.
     */
    private FilesystemHelper fsHelper;
    
    /**
     * Storage Client
     */
    private StorageClient client;
    
    /**
     * The access control manager in use.
     */
    private AccessControlManager accessControlManager;

    private boolean closed;

    private StoreListener eventListener;

    private PathPrincipalTokenResolver pathPrincipalResolver;

    public ContentManagerImpl(CacheContainer cacheContainer, StorageClient client,
        AccessControlManager accessControlManager, Configuration config,
        Map<String, CacheHolder> sharedCache, StoreListener eventListener) {
      super(client, sharedCache);
      this.metaCache = cacheContainer.<String, Metadata>getCache(
          config.getContentMetadataName());
      this.fs = new GridFilesystem(cacheContainer.<String, byte[]>getCache(
          config.getContentBodyCacheName()), metaCache);
      this.client = client;
      closed = false;
      this.eventListener = eventListener;
      String userId = accessControlManager.getCurrentUserId();
      String usersTokenPath = StorageClientUtils.newPath(userId, "private/tokens");
      this.pathPrincipalResolver = new PathPrincipalTokenResolver(usersTokenPath, this);
      this.accessControlManager = new AccessControlManagerTokenWrapper(
          accessControlManager, pathPrincipalResolver);
      this.fsHelper = new FilesystemHelper(fs, metaCache);
    }
  

    public boolean exists(String path) {
        try {
            checkOpen();
            accessControlManager.check(Security.ZONE_CONTENT, path, Permissions.CAN_READ);
            Map<String, Object> props = getFileProperties(path);
            return exists(props);
        } catch (AccessDeniedException e) {
            LOGGER.debug(e.getMessage(), e);
        } catch (StorageClientException e) {
            LOGGER.debug(e.getMessage(), e);
        }
        return false;
    }
    
    public Content get(String path) throws StorageClientException, AccessDeniedException {
        try {
            checkOpen();
            accessControlManager.check(Security.ZONE_CONTENT, path, Permissions.CAN_READ);
            Map<String, Object> props = getFileProperties(path);
            if (exists(props)) {
            	Content contentObject = new Content(path, props);
            	((InternalContent) contentObject).internalize(this, false);
            	return contentObject;
            }
        } catch (AccessDeniedException e) {
            LOGGER.debug(e.getMessage(), e);
        } catch (StorageClientException e) {
            LOGGER.debug(e.getMessage(), e);
        }
        
        return null;
    }

    public Iterator<Content> listChildren(final String path) throws StorageClientException {
    	if (!exists(path))
    		return (new LinkedList<Content>()).iterator();
      
  		final Iterator<String> childPaths = listChildPaths(path);
  		
  		return new PreemptiveIterator<Content>() {
  		  private Content content;
  		  
  			@Override
  			protected boolean internalHasNext() {
  			  if (!childPaths.hasNext())
  			    return false;
  			  try {
  			    content = get(childPaths.next());
  			    return true;
  			  } catch (StorageClientException e) {
            LOGGER.warn("Error getting the next child of "+path, e);
          } catch (AccessDeniedException e) {
            LOGGER.warn("Error getting the next child of "+path, e);
          }
  			  
  			  return false;
  			}
  
  			@Override
  			protected Content internalNext() {
  			    return content;
  			}
  		};
    }

    public Iterator<String> listChildPaths(final String path) throws StorageClientException {
      if (!exists(path))
      	return (new LinkedList<String>()).iterator();
      
      List<String> childrenList = Arrays.asList(loadChildren(path));
      
  		if (childrenList.isEmpty())
  			return (new LinkedList<String>()).iterator();
  		
  		final Iterator<String> children = childrenList.iterator();
  		return new PreemptiveIterator<String>() {
  			private String nextPath;
  			
  			@Override
  			protected boolean internalHasNext() {
  				nextPath = null;
  				while (nextPath == null && children.hasNext()) {
  					String contentPath = StorageClientUtils.newPath(path, children.next());
  					if (exists(contentPath)) {
  						nextPath = contentPath;
  						break;
  					}
  				}
  				if (nextPath == null) {
  					super.close();
  					return false;
  				}
  				return true;
  			}
  
  			@Override
  			protected String internalNext() {
  				return nextPath;
  			}
  		};
    }
    
    public void triggerRefresh(String path) throws StorageClientException, AccessDeniedException {
        Content c = get(path);
        if ( c != null ) {
            eventListener.onUpdate(Security.ZONE_CONTENT, path,  accessControlManager.getCurrentUserId(), getResourceType(c), false, null, "op:update");
        }
    }
    
    private String getResourceType(InternalContent c) {
        String resourceType = null;
        if ( c != null ) {
            if ( c.hasProperty(Content.SLING_RESOURCE_TYPE_FIELD)) {
                resourceType = (String) c.getProperty(Content.SLING_RESOURCE_TYPE_FIELD);
            } else if ( c.hasProperty(Content.RESOURCE_TYPE_FIELD)) {
                resourceType = (String) c.getProperty(Content.RESOURCE_TYPE_FIELD);
            } else if ( c.hasProperty(Content.MIMETYPE_FIELD)) {
                resourceType = (String) c.getProperty(Content.MIMETYPE_FIELD);
            }
        }
        return resourceType;
    }

    public void triggerRefreshAll() throws StorageClientException {
        triggerRefreshAll("/");
    }

    private void triggerRefreshAll(String path) throws StorageClientException {
    	if (User.ADMIN_USER.equals(accessControlManager.getCurrentUserId()) ) { 
    	  String grandchildPrefix = String.format("%s/", getFilesystemPath(path));
    		for (String pathKey : metaCache.keySet()) {
    		  if (pathKey.startsWith(grandchildPrefix)) {
      		  File file = fs.getFile(pathKey);
      		  if (isLiveContentFile(file)) {
      		    try {
                triggerRefresh(getContentPath(file.getAbsolutePath()));
              } catch (AccessDeniedException e) {
                LOGGER.error("Exception while refreshing all content", e);
              }
      		  }
    		  }
    		}
    	}
    }
    
    public void update(Content content) throws AccessDeniedException, StorageClientException {
        update(content, Boolean.TRUE);
    }

    public void update(Content excontent, boolean withTouch) throws AccessDeniedException, StorageClientException {
        checkOpen();
        InternalContent content = (InternalContent) excontent;
        String path = content.getPath();
        accessControlManager.check(Security.ZONE_CONTENT, path, Permissions.CAN_WRITE);
        Map<String, Object> toSave = null;
        // deal with content that already exists, but has been marked as new by merging in the new content.
        // content that is deleted wont appear in this layer
        if (content.isNew()) {
            Content existingContent = get(path);
            if ( existingContent != null ) {
                Map<String, Object> properties = content.getProperties();
                for ( Entry<String, Object> e : properties.entrySet()) {
                   existingContent.setProperty(e.getKey(), e.getValue());
                }
                content = existingContent;
            }
        }

        Map<String, Object> originalProperties = ImmutableMap.of();
        // only admin can bypass the lastModified fields using withTouch=false
        boolean touch = withTouch || !User.ADMIN_USER.equals(accessControlManager.getCurrentUserId());
        boolean isnew = false;
        
        if (content.isNew()) {
          isnew = true;
          // create the parents if necessary
          if (!StorageClientUtils.isRoot(path)) {
              String parentPath = StorageClientUtils.getParentObjectPath(path);
              Content parentContent = get(parentPath);
              if (parentContent == null) {
                  update(new Content(parentPath, null), withTouch);
                }
            } else {
              File root = fs.getFile(FILESYSTEM_ROOT);
              if (!root.exists())
                root.mkdir();
            }
            toSave =  Maps.newHashMap(content.getPropertiesForUpdate());
            
            if (touch) {
              // if the user is admin we allow overwriting of protected fields. This should allow content migration.
              toSave.put(CREATED_FIELD, System.currentTimeMillis());
              toSave.put(CREATED_BY_FIELD, accessControlManager.getCurrentUserId());
              toSave.put(LASTMODIFIED_FIELD, System.currentTimeMillis());
              toSave.put(LASTMODIFIED_BY_FIELD, accessControlManager.getCurrentUserId());
            }
            
            toSave.put(DELETED_FIELD, new RemoveProperty()); // make certain the deleted field is not set
            LOGGER.debug("New Content with {} {} ", path, toSave);
        } else if (content.isUpdated()) {
          originalProperties = content.getOriginalProperties();
          toSave =  Maps.newHashMap(content.getPropertiesForUpdate());

          for (String field : PROTECTED_FIELDS) {
            LOGGER.debug("Resetting value for {} to {}", field, originalProperties.get(field));
            toSave.put(field, originalProperties.get(field));
          }
          
          if (touch) {
            toSave.put(LASTMODIFIED_FIELD, System.currentTimeMillis());
            toSave.put(LASTMODIFIED_BY_FIELD, accessControlManager.getCurrentUserId());
            toSave.put(DELETED_FIELD, new RemoveProperty()); // make certain the deleted field is not set
          }
          LOGGER.debug("Updating Content with {} {} ", path, toSave);
        } else {
            // if not new or updated, don't update.
            return;
        }

        // verify that the file is not read-only
        Map<String, Object> checkContent = getFileProperties(path);
        if (exists(checkContent) && TRUE.equals((String)checkContent.get(READONLY_FIELD))) {
            throw new AccessDeniedException(Security.ZONE_CONTENT, path,
                    "update on read only Content Item (possibly a previous version of the item)",
                    accessControlManager.getCurrentUserId());
        }

        // persist the properties to the file-store
        toSave.put(PATH_FIELD, path);
        putProperties(path, toSave);

        LOGGER.debug("Saved {} as {} ", new Object[] { path, toSave });
        
        // reset state to unmodified to take further modifications.
        content.reset(getFileProperties(path));
        
        eventListener.onUpdate(Security.ZONE_CONTENT, path, accessControlManager.getCurrentUserId(), getResourceType(content),  isnew, originalProperties, "op:update");
    }
    
    /**
     * {@inheritDoc}
     *
     * @see org.sakaiproject.nakamura.api.lite.content.ContentManager#replace(org.sakaiproject.nakamura.api.lite.content.Content)
     */
    // TODO unit test
    public void replace(Content content) throws AccessDeniedException,
        StorageClientException {
      replace(content, true);
    }

    /**
     * {@inheritDoc}
     *
     * @see org.sakaiproject.nakamura.api.lite.content.ContentManager#replace(org.sakaiproject.nakamura.api.lite.content.Content, boolean)
     */
    // TODO unit test
    public void replace(Content content, boolean withTouch)
        throws AccessDeniedException, StorageClientException {
      Content current = get(content.getPath());
      if (current != null) {
        Set<String> diffKeys = diffKeys(current.getProperties(), content.getProperties());
        for (String diffKey : diffKeys) {
          content.setProperty(diffKey, new RemoveProperty());
        }
      }
      update(content, withTouch);
    }

    /**
     * Set the keys in <code>update</code> to <code>new RemoveProperty()</code> if they are
     * in <code>current</code> but not in <code>update</code>. System properties are ignored
     * which is the only difference to {@link StorageClientUtils#diffKeys(Map, Map)}.
     *
     * @param current
     *          The current content found at the location.
     * @param update
     *          The content that will be used to update the location.
     * @return Set of keys to remove from <code>update</code>.
     */
    private Set<String> diffKeys(Map<String, Object> current, Map<String, Object> update) {
      Set<String> diffKeys = StorageClientUtils.diffKeys(current, update);
      if (diffKeys.size() > 0) {
        // remove system properties
        Iterator<String> keysIter = diffKeys.iterator();
        while (keysIter.hasNext()) {
          String diffKey = keysIter.next();
          if (diffKey.startsWith(Repository.SYSTEM_PROP_PREFIX)) {
            keysIter.remove();
          }
        }
      }
      return diffKeys;
    }

    public void delete(String path) throws AccessDeniedException, StorageClientException {
      delete(path, false);
    }
    
    public void delete(String path, boolean recurse) throws AccessDeniedException, StorageClientException {
        checkOpen();
        checkCanDelete(path);
        if (exists(path)) {
          Iterator<String> children = listChildPaths(path);
          if (!recurse && children.hasNext()) {
              throw new StorageClientException("Unable to delete a path with active children ["
                  + path + "]. Set recurse=true to delete a tree.");
          }
          
          // delete and do not keep history
          deleteInternal(path, false);
        }
    }

    /**
     * Recursively delete all content beneath the path, including the path itself.
     * 
     * @param path The path to recursively delete.
     * @param keepHistory Whether or not the keep the history of the deleted nodes.
     * @throws AccessDeniedException 
     * @throws StorageClientException 
     */
    private void deleteInternal(String path, boolean keepHistory) throws StorageClientException,
        AccessDeniedException {
      checkOpen();
      checkCanDelete(path);
      
      if (exists(path)) {
        Iterator<String> children = listChildPaths(path);
        
        // recursively delete all children
        while (children.hasNext()) {
          String child = children.next();
          deleteInternal(child, keepHistory);
        }
        
        LOGGER.info("Deleting path: {}", path);
        
        Content content = get(path);
        Map<String, Object> contentBeforeDelete = content.getProperties();
        String resourceType = (String) contentBeforeDelete.get("sling:resourceType");
        
        softDeleteProperties(path);
        
        eventListener.onDelete(Security.ZONE_CONTENT, path, accessControlManager.getCurrentUserId(), resourceType, contentBeforeDelete);
      }
      
      // streams and versions may be associated to the content even if the content is deleted.
      hardDeleteStreams(path);
      
      if (!keepHistory) {
        hardDeleteVersions(path);
      }
    }
    
    public long writeBody(String path, InputStream in) throws StorageClientException,
            AccessDeniedException, IOException {
        return writeBody(path, in, null);
    }

    public long writeBody(String path, InputStream in, String streamId)
            throws StorageClientException, AccessDeniedException, IOException {
        checkOpen();
        accessControlManager.check(Security.ZONE_CONTENT, path, Permissions.CAN_WRITE);
        
        if (!exists(path)) {
        	update(new Content(path, null));
        }
        
        Content parentContent = get(path);
        File streamFile = getFileFromContentPath(StorageClientUtils.newPath(path,
            getStreamFileNameByStreamId(streamId)));
        streamFile.createNewFile();
        
        OutputStream os = null;
        try {
        	os = fs.getOutput(streamFile.getAbsolutePath());
        	IOUtils.copyLarge(in, os);
        } finally {
        	closeSilent(os);
        }
        
        update(parentContent, true);
        
        if (streamId != null) {
          Map<String, Object> props = getFileProperties(path);
          @SuppressWarnings("unchecked")
          Set<String> streams = (Set<String>) props.get(STREAMS_FIELD);
          if (streams == null) {
            streams = new HashSet<String>();
            streams.add(streamId);
            props.put(STREAMS_FIELD, streams);
            putProperties(path, props);
          } else if (!streams.contains(streamId)){
            streams.add(streamId);
            props.put(STREAMS_FIELD, streams);
            putProperties(path, props);
          }
        }
        
        eventListener.onUpdate(Security.ZONE_CONTENT, path, accessControlManager.getCurrentUserId(), getResourceType(parentContent), false, null, "stream", streamId);
        return streamFile.length();
    }

    public InputStream getInputStream(String path) throws StorageClientException,
            AccessDeniedException, IOException {
        return getInputStream(path, null);
    }

    public InputStream getInputStream(String path, String streamId) throws StorageClientException,
            AccessDeniedException, IOException {
        checkOpen();
        accessControlManager.check(Security.ZONE_CONTENT, path, Permissions.CAN_READ);
        String streamPath = StorageClientUtils.newPath(path, getStreamFileNameByStreamId(streamId));
        File streamFile = getFileFromContentPath(streamPath);
        if (!streamFile.exists())
        	return new ByteArrayInputStream(new byte[0]);
        
        return new BufferedInputStream(new GridFileInputStreamWrapper(
            fs.getInput(streamFile)));
    }

    public void close() {
        closed = true;
    }

    private void checkOpen() throws StorageClientException {
        if (closed) {
            throw new StorageClientException("Content Manager is closed");
        }
    }

    // TODO: Unit test
    /**
     * {@inheritDoc}
     * @see org.sakaiproject.nakamura.api.lite.content.ContentManager#copy(java.lang.String, java.lang.String, boolean)
     */
    public void copy(String from, String to, boolean withStreams) throws StorageClientException,
            AccessDeniedException, IOException {
        checkOpen();
        copyInternal(from, to, withStreams);
        
        Content copiedContent = get(to);
        copiedContent.setProperty(COPIED_FROM_PATH_FIELD, from);
        copiedContent.setProperty(COPIED_DEEP_FIELD, withStreams);
        update(copiedContent);
        
        eventListener.onUpdate(Security.ZONE_CONTENT, to, accessControlManager.getCurrentUserId(),
            getResourceType(copiedContent), true, null, "op:copy");
    }
    
    @SuppressWarnings("unchecked")
    private void copyInternal(String from, String to, boolean withStreams)
        throws StorageClientException, AccessDeniedException, IOException {
      // To Copy, get the to object out and copy everything over.
      Content f = get(from);
      if (f == null) {
          throw new StorageClientException(" Source content " + from + " does not exist");
      }
      Content t = get(to);
      if (t != null) {
         LOGGER.debug("Deleting {} ",to);
         delete(to);
      }
      Set<String> streams = null;
      Map<String, Object> copyProperties = Maps.newHashMap();
      copyProperties.putAll(f.getProperties());
      
      if (withStreams) {
        streams = (Set<String>) f.getProperty(STREAMS_FIELD);
        if (streams == null) {
          streams = Sets.newHashSet();
        }
      } else {
        streams = Sets.newHashSet();
      }
      
      
      t = new Content(to, copyProperties);
      update(t);
      LOGGER.debug("Copy Updated {} {} ",to,t);

      if (withStreams) {
        
        // first copy the default stream if it exists
        InputStream defaultStream = null;
        try {
          defaultStream = getInputStream(from, null);
          if (defaultStream != null) {
            writeBody(to, defaultStream);
          }
        } finally {
          closeSilent(defaultStream);
        }
        
        for (String stream : streams) {
            InputStream fromStream = null;
            try {
              fromStream = getInputStream(from, stream);
              writeBody(to, fromStream, stream);
            } finally {
              closeSilent(fromStream);
            }
        }
      }
    }

    public List<ActionRecord> move(String from, String to) throws AccessDeniedException,
        StorageClientException {
      return move(from, to, false);
    }

    public List<ActionRecord> move(String from, String to, boolean force)
        throws AccessDeniedException, StorageClientException {
      return move (from, to, force, true);
    }
    
    public List<ActionRecord> move(String from, String to, boolean force, boolean keepDestHistory)
        throws AccessDeniedException, StorageClientException {
      checkOpen();
      checkCanMove(from, to);
      
      List<ActionRecord> record = Lists.newArrayList();

      // verify the source exists
      if (!exists(from)) {
        throw new StorageClientException(String.format(
            "The source content to move from %s does not exist, move operation failed", from));
      }

      // verify either the destination does not exist, or we are explicitly overwriting
      if (exists(to) && !force) {
        throw new StorageClientException(String.format(
            "The destination content to move to %s exists, move operation failed", to));
      }
      
      // sanitize the 'keepDestHistory' flag. If the destination does not exist, it is implicitly false
      if (!exists(to)) {
        keepDestHistory = false;
      }
      
      // first recursively move the children
      Iterator<String> iter = listChildPaths(from);
      while (iter.hasNext()) {
        String childPath = iter.next();
        // Since this is a direct child of the previous from, only the last token needs to
        // be appended to "to"
        record.addAll(move(childPath, to.concat(childPath.substring(childPath.lastIndexOf("/"))),
            force, keepDestHistory));
      }
      
      // handle versions
      if (keepDestHistory) {
        hardDeleteVersions(from);
      } else {
        try {
          moveAndReplaceVersions(from, to);
        } catch (IOException e) {
          throw new StorageClientException(String.format(
              "Error transferring versions from '%s' to '%s'", from, to), e);
        }
      }
      
      // handle streams
      try {
        replaceStreams(from, to);
        hardDeleteStreams(from);
      } catch (IOException e) {
        throw new StorageClientException(String.format(
            "Error transferring streams from '%s' to '%s'", from, to), e);
      }
      
      // handle the properties file
      try {
        replaceProperties(from, to);
        softDeleteProperties(from);
      } catch (IOException e) {
        throw new StorageClientException(String.format(
            "Error copying properties from '%s' to '%s'", from, to), e);
      }

      try {
        moveAcl(from, to, force);
      } catch (AccessDeniedException e) {
        /*
         * It should be acceptable to move content without transferring any ACLs. ACLs that
         * existed before (if any) will be maintained.
         */
        LOGGER.debug("Moved content without proper permission to transfer ACLs.");
      }

      // update the indexes by removing the old and updating the new
      Content content = get(to);
      client.removeIndex(from, content.getProperties());
      client.updateIndex(to, content.getProperties());
      
      // move does not add resourceTypes to events.
      eventListener.onUpdate(Security.ZONE_CONTENT, to, accessControlManager.getCurrentUserId(), null, true, null, "op:move");
      eventListener.onDelete(Security.ZONE_CONTENT, from, accessControlManager.getCurrentUserId(), null, null, "op:move");
      record.add(new ActionRecord(from, to));
      
      return record;
    }

    public String saveVersion(String path) throws StorageClientException, AccessDeniedException {
        return saveVersion(path, null);
    }

    public String saveVersion(String path, Map<String, Object> versionMetadata) throws StorageClientException, AccessDeniedException {
        checkOpen();
        accessControlManager.check(Security.ZONE_CONTENT, path, Permissions.CAN_WRITE);
        if (!exists(path)) {
            throw new StorageClientException("Item "+path+" does not exist");
        }

        List<String> versions = getVersionHistory(path);
        String nextVersionNumber = String.valueOf(versions.size()+1);
        String nextVersionDirName = StorageClientUtils.newPath(path,
        		getVersionFileNameByVersionNumber(nextVersionNumber));
        
        
        File nextVersionDir = getFileFromContentPath(nextVersionDirName);
        nextVersionDir.mkdir();
        
        // simply copy into the next version node
        try {
          replaceProperties(path, nextVersionDirName);
          replaceStreams(path, nextVersionDirName);
        } catch (IOException e) {
        	throw new StorageClientException("Error trying to store content version.", e);
        }
        
        Content savedVersion = get(nextVersionDirName);
        
        // apply the content metadata
        if (versionMetadata != null) {
          for (Map.Entry<String, Object> entry : versionMetadata.entrySet()) {
            String key = String.format("%s:%s", PREFIX_PROP_VERSION_METADATA, entry.getKey());
            savedVersion.setProperty(key, entry.getValue());
          }
        }
        
        // make version read-only
        savedVersion.setProperty(READONLY_FIELD, TRUE);
        putProperties(savedVersion.getPath(), savedVersion.getProperties());
        
        return nextVersionNumber;
    }

    public List<String> getVersionHistory(String path) throws AccessDeniedException,
            StorageClientException {
        checkOpen();
        accessControlManager.check(Security.ZONE_CONTENT, path, Permissions.CAN_READ);
        if (exists(path)) {
          // discover versions by incrementing a version number counter
          List<Integer> versionHistory = getVersionNumbers(path);
          // sort the ints and turn it into a string list
          Collections.sort(versionHistory, Collections.reverseOrder());
          List<String> versionHistoryStr = new LinkedList<String>();
          for (Integer version : versionHistory) {
          	versionHistoryStr.add(String.valueOf(version));
          }
            
          return versionHistoryStr;
        }
        return Collections.emptyList();
    }

    // TODO: Unit test
    public InputStream getVersionInputStream(String path, String versionId)
            throws AccessDeniedException, StorageClientException, IOException {
        return getVersionInputStream(path, versionId, null);
    }

    // TODO: Unit test
    public InputStream getVersionInputStream(String path, String versionId, String streamId)
            throws AccessDeniedException, StorageClientException, IOException {
      accessControlManager.check(Security.ZONE_CONTENT, path, Permissions.CAN_READ);
      checkOpen();
      if (exists(path)) {
	    	String versionDir = getVersionFileNameByVersionNumber(versionId);
	    	return getInputStream(StorageClientUtils.newPath(path, versionDir), streamId);
      }
      return null;
    }

    public Content getVersion(String path, String versionNumber) throws StorageClientException,
            AccessDeniedException {
        checkOpen();
        accessControlManager.check(Security.ZONE_CONTENT, path, Permissions.CAN_READ);
        Content result = null;
        if (exists(path)) {
        	String versionDirName = StorageClientUtils.newPath(path,
        			getVersionFileNameByVersionNumber(versionNumber));
        	if (exists(versionDirName)) {
        		Map<String, Object> versionProps = getFileProperties(versionDirName);
        		LOGGER.debug("Found version content at path '{}': {}", versionDirName, versionProps);
        		result = new Content(path, versionProps);
        		((InternalContent) result).internalize(this, true);
        	} else {
        		LOGGER.debug("No version for path '{}': version history null at path '{}'", path,
        				versionDirName);
        	}
        } else {
        	LOGGER.debug("No live content found for path '{}'", path);
        }
        return result;
    }

    @Override
    protected Logger getLogger() {
        return LOGGER;
    }

    public Iterable<Content> find(final Query query, final Sort sort) throws StorageClientException,
        AccessDeniedException {
      checkOpen();
      return new Iterable<Content>() {
        public Iterator<Content> iterator() {
            Iterator<Content> contentResultsIterator = null;
            try {
              final QueryIterator documents = client.find(query, sort);
              
              contentResultsIterator = new PreemptiveIterator<Content>() {
                  private Content contentResult;

                  protected boolean internalHasNext() {
                      contentResult = null;
                      while (contentResult == null && documents.hasNext()) {
                          try {
                              IndexDocument document = (IndexDocument) documents.next();
                              LOGGER.debug("Loaded Next as {} ", document);
                              if ( exists(document.getId()) ) {
                                  String path = document.getId();
                                  contentResult = get(path);
                              }
                          } catch (AccessDeniedException e) {
                              LOGGER.debug(e.getMessage(),e);
                          } catch (StorageClientException e) {
                              LOGGER.debug(e.getMessage(),e);
                          }
                      }
                      if (contentResult == null) {
                          close();
                          return false;
                      }
                      return true;
                  }

                  protected Content internalNext() {
                      return contentResult;
                  }
                  
                  @Override
                  public void close() {
                    documents.close();
                    super.close();
                  };
              };
            } catch (StorageClientException e) {
              LOGGER.error("Unable to iterate over sparsemap search results.", e);
            }
            return contentResultsIterator;
        }
    };
    }
    
    public int count(Query query) throws StorageClientException {
      return client.count(query);
    }


    public boolean hasBody(String path, String streamId) throws StorageClientException, AccessDeniedException {
        if (!exists(path)) {
          return false;
        }
        String streamPath = StorageClientUtils.newPath(path, getStreamFileNameByStreamId(streamId));
        File streamFile = getFileFromContentPath(streamPath);
        return streamFile.exists();
    }

    public void setPrincipalTokenResolver(PrincipalTokenResolver principalTokenResolver) {
        accessControlManager.setRequestPrincipalResolver(principalTokenResolver);
    }

    public void cleanPrincipalTokenResolver() {
        accessControlManager.clearRequestPrincipalResolver();
    }


    /**
     * Move ACLs from source to destination. This mirrors the move functionality found in
     * {@link ContentManager}.
     *
     * @param from
     *          The source path where the ACLs are applied.
     * @param to
     *          The source path where the ACLs are to be applied.
     * @param force
     *          Whether to forcefully move to the destination (i.e. overwrite)
     * @return
     * @throws AccessDeniedException
     * @throws StorageClientException
     * @see Security#ZONE_ADMIN, Security#ZONE_AUTHORIZABLES, Security#ZONE_CONTENT
     */
    private boolean moveAcl(String from, String to, boolean force)
      throws AccessDeniedException, StorageClientException {
      String objectType = Security.ZONE_CONTENT;
      boolean moved = false;

      checkCanMoveAcl(from, to);

      // get the ACL to move and make the map mutable
      Map<String, Object> fromAcl = Maps.newHashMap(accessControlManager.getAcl(objectType, from));
      if (fromAcl != null) {

        // remove the read-only properties to be re-added when setting the new acl
        for (String readOnly : ACL_READ_ONLY_PROPERTIES) {
          fromAcl.remove(readOnly);
        }

        // check for a destination if necessary
        if (!force && !accessControlManager.getAcl(objectType, to).isEmpty()) {
          throw new StorageClientException("The destination ACL {" + to
              + "} exists, move operation failed");
        }

        // parse the ACL and create modifications for the `to` location
        List<AclModification> modifications = Lists.newArrayList();
        for (Entry<String, Object> fromAce : fromAcl.entrySet()) {
          String aceKey = fromAce.getKey();
          Object aceValue = fromAce.getValue();
          if (aceValue != null) {
            try {
              int bitmap = (Integer) aceValue;
              modifications.add(new AclModification(aceKey, bitmap, Operation.OP_REPLACE));
            } catch (NumberFormatException e) {
              LOGGER.info("Skipping corrupt ACE value {} at {}->{}", new Object[] {
                  aceValue, from, to });
            }
          }
        }

        // set the ACL on the `to` path
        AclModification[] mods = modifications.toArray(new AclModification[modifications.size()]);
        accessControlManager.setAcl(objectType, to, mods);

        // remove the old ACLs on the `from` path
        for (int i = 0; i < mods.length; i++) {
          mods[i] = new AclModification(mods[i].getAceKey(), 0, Operation.OP_DEL);
        }
        accessControlManager.setAcl(objectType, from, mods);

        moved = true;
      }
      return moved;
    }
    
    /**
     * Determines whether or not the current user can move the object from the given {@code from}
     * path to the given {@code to} path.
     * 
     * @param from
     * @param to
     * @throws AccessDeniedException
     * @throws StorageClientException
     */
    private void checkCanMove(String from, String to) throws AccessDeniedException,
        StorageClientException {
      accessControlManager.check(Security.ZONE_CONTENT, from,
          Permissions.CAN_READ.combine(Permissions.CAN_WRITE));
      accessControlManager.check(Security.ZONE_CONTENT, to,
              Permissions.CAN_READ.combine(Permissions.CAN_WRITE));
      checkCanDelete(from);
      
      /*
       * It's worth noting that we're not checking checkCanDelete on the 'to' path. This is because
       * when moving content to the 'to' path, that specific node is never actually being deleted,
       * just edited/replaced. Since we consider any children of a write-able node as delete-able
       * (see checkCanDelete), checking CAN_WRITE on the 'to' path is enough.
       */
    }
    
    /**
     * Determines whether or not the current user has the rights to move ACLs from the given
     * {@code from} path to the given {@code to} path.
     * 
     * @param from
     * @param to
     * @throws AccessDeniedException
     * @throws StorageClientException
     */
    private void checkCanMoveAcl(String from, String to) throws AccessDeniedException,
        StorageClientException {
      // we will be READing, from the source, then deleting from the source 
      accessControlManager.check(Security.ZONE_CONTENT, from, Permissions.CAN_READ_ACL.combine(Permissions.CAN_WRITE_ACL));
      
      // we will be WRITEing to the destination, but we will not be deleting existing ACLs at this point.
      accessControlManager.check(Security.ZONE_CONTENT, to, Permissions.CAN_WRITE_ACL);
    }
    
    /**
     * Determines whether or not the current user can delete the object at the given {@code path}.
     * 
     * @param path
     * @throws AccessDeniedException If the user cannot delete the given node.
     * @throws StorageClientException If there is an generic error accessing the storage client.
     */
    private void checkCanDelete(String path) throws AccessDeniedException, StorageClientException {
      if (StorageClientUtils.isRoot(path)) {
        // if this is a root path, no check to the parent is required
        accessControlManager.check(Security.ZONE_CONTENT, path, Permissions.CAN_DELETE);
      } else {
        // we first check the parent to see if the user has write access on it. if they do, then
        // they are allowed to delete this child.
        String parentPath = StorageClientUtils.getParentObjectPath(path);
        try {
          accessControlManager.check(Security.ZONE_CONTENT, parentPath, Permissions.CAN_WRITE);
        } catch (AccessDeniedException e) {
          // the user cannot write the parent, but if they can delete the current, then we succeed
          accessControlManager.check(Security.ZONE_CONTENT, path, Permissions.CAN_DELETE);
        }
      }
    }
    
    /**
     * TODO: Probably a good idea to cache the serialized content objects, instead of rely entirely
     * on the distributed binary body cache.
     * 
     * @param path the content path of the file to get
     * @return
     */
    private Map<String, Object> getFileProperties(String path) {
      File directory = getFileFromContentPath(path);
      if (!directory.exists())
      	return null;
        
      File propertiesFile = getFileFromContentPath(StorageClientUtils.newPath(path,
          FILE_PROPERTIES));
      if (!propertiesFile.exists())
      	return ImmutableMap.of();
        
      ObjectInputStream pin = null;
      try {
      	pin = new ObjectInputStream(fs.getInput(propertiesFile));
      	@SuppressWarnings("unchecked")
        Map<String, Object> result = (Map<String, Object>) pin.readObject();
      	
      	return result;
      } catch (IOException e) {
      	LOGGER.warn("Received exception trying to load file properties.", e);
      } catch (ClassNotFoundException e) {
      	LOGGER.warn("Received exception trying to load file properties.", e);
  		} finally {
      	closeSilent(pin);
      }
      return null;
    }
    
    /**
     * Update the child listing of the parent content of the given {@code path} to ensure
     * it is aware of the child.
     * 
     * @param path
     * @throws StorageClientException 
     */
    private void updateParentChild(String path) throws StorageClientException {
      String parent = StorageClientUtils.getParentObjectPath(path);
      String child = StorageClientUtils.getObjectName(path);
      Set<String> children = new HashSet<String>(Arrays.asList(loadChildren(parent)));
      if (!children.contains(child)) {
        children.add(child);
        putChildren(parent, children.toArray(new String[children.size()]));
      }
    }
    
    /**
     * Update the child listing of the parent content of the given {@code path} to ensure
     * it no longer maintains a reference of the child.
     * 
     * @param path
     * @throws StorageClientException
     */
    private void removeParentChild(String path) throws StorageClientException {
      String parent = StorageClientUtils.getParentObjectPath(path);
      String child = StorageClientUtils.getObjectName(path);
      Set<String> children = new HashSet<String>(Arrays.asList(loadChildren(parent)));
      if (children.contains(child)) {
        children.remove(child);
        putChildren(parent, children.toArray(new String[children.size()]));
      }
    }
    
    private void putChildren(String path, String[] children) throws StorageClientException {
      File childrenFile = getFileFromContentPath(path, FILE_CHILDREN);
      if (children == null || children.length == 0) {
        childrenFile.delete();
      }
      
      ObjectOutputStream oos = null;
      try {
        childrenFile.createNewFile();
        oos = new ObjectOutputStream(fs.getOutput(childrenFile.getAbsolutePath()));
        oos.writeObject(children);
      } catch (IOException e) {
        LOGGER.error("Error persistent children list to file {}", childrenFile.getAbsoluteFile());
        throw new StorageClientException("Error persisting children.", e);
      } finally {
        closeSilent(oos);
      }
    }
    
    private String[] loadChildren(String path) throws StorageClientException {
      if (!exists(path))
        return new String[0];
      
      File childrenFile = getFileFromContentPath(path, FILE_CHILDREN);
      if (!childrenFile.exists())
        return new String[0];
      
      ObjectInputStream ois = null;
      try {
        ois = new ObjectInputStream(fs.getInput(childrenFile));
        
        String[] children = (String[]) ois.readObject();
        if (children == null)
          children = new String[0];
        
        return children; 
      } catch (FileNotFoundException e) {
        LOGGER.error("Could not find binary children file to read: {}", childrenFile.getAbsolutePath());
        throw new StorageClientException("Error loading children of content: "+path, e);
      } catch (IOException e) {
        LOGGER.error("Could not load binary children file to read: {}", childrenFile.getAbsolutePath());
        throw new StorageClientException("Error loading children of content: "+path, e);
      } catch (ClassNotFoundException e) {
        LOGGER.error("Could not load binary children file to read: {}", childrenFile.getAbsolutePath());
        throw new StorageClientException("Error loading children of content: "+path, e);
      } finally {
        closeSilent(ois);
      }
    }
    
    private void putProperties(String path, Map<String, Object> toSave)
        throws StorageClientException, AccessDeniedException {
      
      // ensure the map is write-able, because we aren't done updating yet
      toSave = new HashMap<String, Object>(toSave);
      
      // ensure explicitly deleted entries are deleted
      Set<String> toRemove = new HashSet<String>();
      for (Map.Entry<String, Object> entry : toSave.entrySet()) {
        if (entry.getValue() instanceof RemoveProperty) {
          toRemove.add(entry.getKey());
        } else if (entry.getValue() == null) {
          toRemove.add(entry.getKey());
        }
      }
      
      for (String key : toRemove) {
        toSave.remove(key);
      }
      
      toSave.put(PATH_FIELD, path);
      
        try {
          // values of this content could have changed that unbinds it from indexers. So we should
          // explicitly remove the previous version from the index before updating the new version
          Content previousContent = get(path);
          if (previousContent != null) {
            client.removeIndex(path, previousContent.getProperties());
          }

	        File dir = getFileFromContentPath(path);
	        dir.mkdir();
	        
	        File props = getFileFromContentPath(StorageClientUtils.newPath(path, FILE_PROPERTIES));
	        props.createNewFile();
	        
	        ObjectOutputStream os = null;
	        try {
	        	os = new ObjectOutputStream(fs.getOutput(props.getAbsolutePath()));
	        	os.writeObject(toSave);
	        } finally {
	        	closeSilent(os);
	        }
	        
          // update the index
          client.updateIndex(path, toSave);
          if (previousContent == null && exists(toSave)) {
            LOGGER.info("Indexing parent-child relationship for {}", path);
            updateParentChild(path);
          }
          
        } catch (IOException e) {
        	throw new StorageClientException(String.format("Error persisting content at '%s'.", path), e);
        }
    }
    
    /**
     * Mark the internal properties file as deleted.
     * 
     * @param path
     * @throws StorageClientException
     * @throws AccessDeniedException
     */
    private void softDeleteProperties(String path) throws StorageClientException,
        AccessDeniedException {
      if (exists(path)) {
        putProperties(path, ImmutableMap.of(DELETED_FIELD, (Object) TRUE));
        LOGGER.info("Removing parent-child index for {}", path);
        removeParentChild(path);
      }
    }
    
    /**
     * Permanently delete the properties file associated to the given content {@code path}.
     * 
     * @param path
     * @throws StorageClientException 
     */
    private void hardDeleteProperties(String path) throws StorageClientException {
      File propertiesFile = getFileFromContentPath(path, FILE_PROPERTIES);
      if (propertiesFile.exists())
        propertiesFile.delete();
      
      // ensure the index does not have this document
      LOGGER.info("Removing parent-child index for {}", path);
      removeParentChild(path);
    }
    
    /**
     * Replace the properties at the {@code to} content path with those at the {@code from} content
     * path.
     * 
     * @param from
     * @param to
     * @throws IOException
     */
    private void replaceProperties(String from, String to) throws StorageClientException,
        IOException {
      String fromFilesystemPath = getFilesystemPath(from);
      String toFilesystemPath = getFilesystemPath(to);
      
      String fromPropertiesFilePath = StorageClientUtils.newPath(fromFilesystemPath,
          FILE_PROPERTIES);
      String toPropertiesFilePath = StorageClientUtils.newPath(toFilesystemPath,
          FILE_PROPERTIES);
      fsHelper.copyFile(fromPropertiesFilePath, toPropertiesFilePath);
      
      // by copying the properties file from one location to another, we have potentially a new content.
      LOGGER.info("Indexing parent-child relationship for {}", to);
      updateParentChild(to);
    }

    /**
     * Permanently delete the streams that are associated with the content at the given content
     * path.
     * 
     * @param path
     * @throws AccessDeniedException 
     * @throws StorageClientException 
     */
    private void hardDeleteStreams(String path) throws StorageClientException, AccessDeniedException {
      // delete the named streams
      Map<String, Object> properties = getFileProperties(path);
      if (properties != null) {
        @SuppressWarnings("unchecked")
        Set<String> streams = (Set<String>) properties.get(STREAMS_FIELD);
        if (streams != null) {
          for (String streamId : streams) {
            File streamFile = getFileFromContentPath(path, getStreamFileNameByStreamId(streamId));
            if (streamFile.exists()) {
              streamFile.delete();
            }
            streams.remove(streamId);
          }
          // the streams set has changed, re-persist this guy.
          putProperties(path, properties);
        }
      }
      
      // delete the "default" stream
      File streamFile = getFileFromContentPath(path, PREFIX_STREAM);
      if (streamFile.exists())
        streamFile.delete();
    }
    
    /**
     * Replace the streams at the {@code to} content path with those at the {@code from} content
     * path.
     * 
     * @param from
     * @param to
     * @throws IOException 
     * @throws AccessDeniedException 
     * @throws StorageClientException 
     */
    private void replaceStreams(String from, String to) throws IOException, StorageClientException, AccessDeniedException {
      hardDeleteStreams(to);
      
      // replace the named streams
      Map<String, Object> fromProps = getFileProperties(from);
      if (fromProps != null) {
        @SuppressWarnings("unchecked")
        Set<String> streams = (Set<String>)fromProps.get(STREAMS_FIELD);
        if (streams != null) {
          for (String streamId : streams) {
            String fromStreamPath = StorageClientUtils.newPath(getFilesystemPath(from),  streamId);
            String toStreamPath = StorageClientUtils.newPath(getFilesystemPath(to),  streamId);
            fsHelper.copyFile(fromStreamPath, toStreamPath);
          }
          Map<String, Object> toProps = getFileProperties(to);
          toProps.put(STREAMS_FIELD, streams);
          putProperties(to, toProps);
        }
      }
      
      // replace the default stream
      File fromStream = getFileFromContentPath(from, PREFIX_STREAM);
      if (fromStream.exists()) {
        String toStreamPath = StorageClientUtils.newPath(getFilesystemPath(to),
            PREFIX_STREAM);
        fsHelper.copyFile(fromStream.getAbsolutePath(), toStreamPath);
      }
    }
    
    /**
     * Permanently delete the versions from the file-system that are associated with the content
     * at the given content path.
     * 
     * @param path
     */
    private void hardDeleteVersions(String path) {
      // loop through possible version names until we find one that doesn't exist.
      for (String versionFilesystemPath : getVersionFilePaths(path)) {
        fsHelper.deleteAll(versionFilesystemPath);
      }
    }
    
    /**
     * Replace the versions at the {@code to} content path with those at the {@code from} content
     * path.
     * 
     * @param from
     * @param to
     * @throws IOException
     * @throws AccessDeniedException 
     * @throws StorageClientException 
     */
    private void moveAndReplaceVersions(String from, String to) throws IOException,
        StorageClientException, AccessDeniedException {
      hardDeleteVersions(to);
      for (String fromVersionPath : getVersionFilePaths(from)) {
        File fromVersion = fs.getFile(fromVersionPath);
        String fromVersionContentPath = StorageClientUtils.newPath(from,
            fromVersion.getName());
        String toVersionContentPath = StorageClientUtils.newPath(to,
            fromVersion.getName());
        replaceStreams(fromVersionContentPath, toVersionContentPath);
        replaceProperties(fromVersionContentPath, toVersionContentPath);
        hardDeleteStreams(fromVersionContentPath);
        hardDeleteProperties(fromVersionContentPath);
        File versionFile = fs.getFile(fromVersionPath);
        versionFile.delete();
      }
    }
    
    /**
     * Get all version directory file-system paths for the content located at the given
     * {@code contentPath}.
     * 
     * @param contentPath
     * @return
     */
    private List<String> getVersionFilePaths(String contentPath) {
      List<String> versionPaths = new LinkedList<String>();
      List<Integer> versionNumbers = getVersionNumbers(contentPath);
      for (Integer versionNumber : versionNumbers) {
        String versionPath = StorageClientUtils.newPath(getFilesystemPath(contentPath),
            getVersionFileNameByVersionNumber(String.valueOf(versionNumber)));
        versionPaths.add(versionPath);
      }
      return versionPaths;
    }
    
    private List<Integer> getVersionNumbers(String contentPath) {
      int versionNumber = 1;
      File versionFile = getFileFromContentPath(contentPath,
          getVersionFileNameByVersionNumber(String.valueOf(versionNumber)));
      List<Integer> versionNumbers = new LinkedList<Integer>();
      while (versionFile.exists()) {
        versionNumbers.add(versionNumber);
        versionNumber++;
        versionFile = getFileFromContentPath(contentPath,
            getVersionFileNameByVersionNumber(String.valueOf(versionNumber)));
      }
      return versionNumbers;
    }

    private File getFileFromContentPath(String contentPath) {
      return fs.getFile(getFilesystemPath(contentPath));
    }
    
    private File getFileFromContentPath(String contentPath, String child) {
      return fs.getFile(getFilesystemPath(contentPath), child);
    }
    
    private boolean exists(Map<String, Object> map) {
        return map != null && map.size() > 0 && !TRUE.equals(map.get(DELETED_FIELD));
    }
    
    private String getStreamFileNameByStreamId(String streamId) {
    	if (StringUtils.isEmpty(streamId))
    		return FILE_STREAM_DEFAULT;
    	return String.format("%s:%s", PREFIX_STREAM, streamId);
    }
    
    private String getVersionFileNameByVersionNumber(String versionNumber) {
    	return String.format("%s:%s", PREFIX_VERSION, versionNumber);
    }
    
    private String getFilesystemPath(String contentPath) {
      String filesystemPath = contentPath;
      if (!contentPath.startsWith(FILESYSTEM_ROOT)) {
        filesystemPath = StorageClientUtils.newPath(FILESYSTEM_ROOT, contentPath);
      }
      return filesystemPath;
    }
    
    private String getContentPath(String filesystemPath) {
      if (filesystemPath == null)
        return null;
      if (!filesystemPath.startsWith(FILESYSTEM_ROOT))
        return filesystemPath;
      return filesystemPath.substring(FILESYSTEM_ROOT.length());
    }
    
    /**
     * Determine if the given file represents a piece of live content.
     * 
     * @param maybeContent
     * @return
     */
    private boolean isLiveContentFile(File maybeContent) {
      if (maybeContent == null)
        return false;
      // a file is a live content if it exists, if it is a directory, and if it is not versioned content
      return maybeContent.exists() && maybeContent.isDirectory() &&
          !maybeContent.getAbsolutePath().contains(PREFIX_VERSION);
    }
    
    private void closeSilent(InputStream is) {
    	try {
    		if (is != null) {
    			is.close();
    		}
    	} catch (IOException e) {
    		LOGGER.warn("Failed to close input stream.", e);
    	}
    }
    
    private void closeSilent(OutputStream os) {
    	try {
    		if (os != null) {
    			os.close();
    		}
    	} catch (IOException e) {
    		LOGGER.warn("Failed to close output stream.", e);
    	}
    }
}
