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
import static org.sakaiproject.nakamura.lite.content.InternalContent.COPIED_FROM_ID_FIELD;
import static org.sakaiproject.nakamura.lite.content.InternalContent.COPIED_FROM_PATH_FIELD;
import static org.sakaiproject.nakamura.lite.content.InternalContent.CREATED_BY_FIELD;
import static org.sakaiproject.nakamura.lite.content.InternalContent.CREATED_FIELD;
import static org.sakaiproject.nakamura.lite.content.InternalContent.DELETED_FIELD;
import static org.sakaiproject.nakamura.lite.content.InternalContent.LASTMODIFIED_BY_FIELD;
import static org.sakaiproject.nakamura.lite.content.InternalContent.LASTMODIFIED_FIELD;
import static org.sakaiproject.nakamura.lite.content.InternalContent.PATH_FIELD;
import static org.sakaiproject.nakamura.lite.content.InternalContent.READONLY_FIELD;
import static org.sakaiproject.nakamura.lite.content.InternalContent.TRUE;
import static org.sakaiproject.nakamura.lite.content.InternalContent.UUID_FIELD;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.infinispan.io.GridFilesystem;
import org.sakaiproject.nakamura.api.lite.CacheHolder;
import org.sakaiproject.nakamura.api.lite.Configuration;
import org.sakaiproject.nakamura.api.lite.RemoveProperty;
import org.sakaiproject.nakamura.api.lite.StorageClientException;
import org.sakaiproject.nakamura.api.lite.StorageClientUtils;
import org.sakaiproject.nakamura.api.lite.StorageConstants;
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
import org.sakaiproject.nakamura.lite.storage.spi.DisposableIterator;
import org.sakaiproject.nakamura.lite.storage.spi.StorageClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

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


    private static final Set<String> PROTECTED_FIELDS = ImmutableSet.of(LASTMODIFIED_FIELD,
                                                                        LASTMODIFIED_BY_FIELD,
                                                                        UUID_FIELD,
                                                                        PATH_FIELD);

    // These properties copied from AccessControlManager to keep from binding
    // directly to the implementation class. They should stay in sync.
    private static final String _SECRET_KEY = "_secretKey";
    private static final String _PATH = "_aclPath";
    private static final String _OBJECT_TYPE = "_aclType";
    private static final String _KEY = "_aclKey";
    private static final Set<String> ACL_READ_ONLY_PROPERTIES = ImmutableSet.of(_SECRET_KEY, _PATH, _OBJECT_TYPE, _KEY);
    
    private static final String PREFIX_CONTENT = "oae";
    private static final String PREFIX_STREAM = String.format("%s:%s", PREFIX_CONTENT, "stream");
    private static final String PREFIX_VERSION = String.format("%s:%s", PREFIX_CONTENT, "version");
    
    private static final String FILE_PROPERTIES = String.format("%s:%s", PREFIX_CONTENT, "props");
    private static final String FILE_STREAM_DEFAULT = PREFIX_STREAM;

    private GridFilesystem fs;
    
    /**
     * Storage Client
     */
    private StorageClient client;
    /**
     * The access control manager in use.
     */
    private AccessControlManager accessControlManager;
    /**
     * Key space for this content.
     */
    private String keySpace;
    
    private String contentColumnFamily;
    
    private boolean closed;

    private StoreListener eventListener;
 

    private PathPrincipalTokenResolver pathPrincipalResolver;

    public ContentManagerImpl(GridFilesystem fs, StorageClient client, AccessControlManager accessControlManager,
            Configuration config,  Map<String, CacheHolder> sharedCache, StoreListener eventListener) {
        super(client, sharedCache);
        this.fs = fs;
        this.client = client;
        keySpace = config.getKeySpace();
        contentColumnFamily = config.getContentColumnFamily();
        closed = false;
        this.eventListener = eventListener;
        String userId = accessControlManager.getCurrentUserId();
        String usersTokenPath = StorageClientUtils.newPath(userId, "private/tokens");
        this.pathPrincipalResolver = new PathPrincipalTokenResolver(usersTokenPath, this);
        this.accessControlManager = new AccessControlManagerTokenWrapper(accessControlManager, pathPrincipalResolver);
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
        checkOpen();
        accessControlManager.check(Security.ZONE_CONTENT, path, Permissions.CAN_READ);
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


    public Iterator<Content> listChildren(String path) throws StorageClientException {
    	if (!exists(path))
    		return (new LinkedList<Content>()).iterator();
    	
		final String[] children = fs.getFile(path).list();
		if (ArrayUtils.isEmpty(children))
			return (new LinkedList<Content>()).iterator();
		
		return new PreemptiveIterator<Content>() {
			int i = 0;
			private Content content;
			
			@Override
			protected boolean internalHasNext() {
				content = null;
				while (content == null && i < children.length) {
					int current = i++;
					try {
						content = get(children[current]);
					} catch (StorageClientException e) {
						LOGGER.debug("Generic error iterating over child {}", children[current]);
					} catch (AccessDeniedException e) {
						LOGGER.debug("Access denied iterating over child {}", children[current]);
					}
				}
				if (content == null) {
					super.close();
					return false;
				}
				return true;
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
        
        final String[] children = fs.getFile(path).list();
		if (ArrayUtils.isEmpty(children))
			return (new LinkedList<String>()).iterator();
		
		return new PreemptiveIterator<String>() {
			int i = 0;
			private String nextPath;
			
			@Override
			protected boolean internalHasNext() {
				nextPath = null;
				while (nextPath == null && i < children.length) {
					int current = i++;
					if (exists(children[current])) {
						nextPath = children[current];
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
    		File file = fs.getFile(path);
    		FileFilter liveContentFileFilter = new LiveContentFileFilter();
    		for (File child : file.listFiles(liveContentFileFilter)) {
				triggerRefreshAll(child.getAbsolutePath());
    		}
    		try {
	    		triggerRefresh(path);
    		} catch (AccessDeniedException e) {
    			LOGGER.error("Exception while refreshing all content", e);
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
            }
            toSave =  Maps.newHashMap(content.getPropertiesForUpdate());
            // if the user is admin we allow overwriting of protected fields. This should allow content migration.
            toSave.put(CREATED_FIELD,
                    touch ? System.currentTimeMillis() : content.getProperty(CREATED_FIELD));
            toSave.put(CREATED_BY_FIELD,
                    touch ? accessControlManager.getCurrentUserId() : content.getProperty(CREATED_BY_FIELD));
            toSave.put(LASTMODIFIED_FIELD,
                    touch ? System.currentTimeMillis() : content.getProperty(LASTMODIFIED_FIELD));
            toSave.put(LASTMODIFIED_BY_FIELD,
                    touch? accessControlManager.getCurrentUserId() : content.getProperty(LASTMODIFIED_BY_FIELD));
            toSave.put(DELETED_FIELD, new RemoveProperty()); // make certain the deleted field is not set
            LOGGER.debug("New Content with {} {} ", path, toSave);
        } else if (content.isUpdated()) {
            originalProperties = content.getOriginalProperties();
            toSave =  Maps.newHashMap(content.getPropertiesForUpdate());


          // only admin can bypass the lastModified fields using withTouch=false
          if (touch) {
            for (String field : PROTECTED_FIELDS) {
              LOGGER.debug("Resetting value for {} to {}", field, originalProperties.get(field));
              toSave.put(field, originalProperties.get(field));
            }
            toSave.put(LASTMODIFIED_FIELD, System.currentTimeMillis());
            toSave.put(LASTMODIFIED_BY_FIELD,
                    accessControlManager.getCurrentUserId());
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
        putProperties(path, toSave);
        
        LOGGER.debug("Saved {} as {} ", new Object[] { path, toSave });
        
        // reset state to unmodified to take further modifications.
        content.reset(getFileProperties(path));
        
        eventListener.onUpdate(Security.ZONE_CONTENT, path, accessControlManager.getCurrentUserId(), getResourceType(content),  isnew, originalProperties, "op:update");
    }
    

    public void delete(String path) throws AccessDeniedException, StorageClientException {
        checkOpen();
        accessControlManager.check(Security.ZONE_CONTENT, path, Permissions.CAN_DELETE);
        if (exists(path)) {
        	Content content = get(path);
        	Map<String, Object> contentBeforeDelete = content.getProperties();
        	String resourceType = (String) contentBeforeDelete.get("sling:resourceType");
        	putProperties(path, ImmutableMap.of(DELETED_FIELD, (Object) TRUE));
        	eventListener.onDelete(Security.ZONE_CONTENT, path, accessControlManager.getCurrentUserId(), resourceType, contentBeforeDelete);
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
        File streamFile = fs.getFile(StorageClientUtils.newPath(path, getStreamFileNameByStreamId(streamId)));
        streamFile.createNewFile();
        
        OutputStream os = null;
        try {
        	os = new FileOutputStream(streamFile);
        	IOUtils.copy(in, os);
        } finally {
        	closeSilent(os);
        }
        
        update(parentContent, true);
        
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
        File streamFile = fs.getFile(streamPath);
        if (!streamFile.exists())
        	return null;
        return new FileInputStream(streamFile);
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
        Set<String> streams = Sets.newHashSet();
        Map<String, Object> copyProperties = Maps.newHashMap();
        copyProperties.putAll(f.getProperties());
        
        if (withStreams) {
        	StreamFileFilter streamFileFilter = new StreamFileFilter();
        	File fromFile = fs.getFile(from);
            for (File streamFile : fromFile.listFiles(streamFileFilter)) {
        		streams.add(streamFileFilter.getStreamId(streamFile.getName()));
            }
        }
        
        copyProperties.put(COPIED_FROM_PATH_FIELD, from);
        copyProperties.put(COPIED_FROM_ID_FIELD, f.getProperty(UUID_FIELD));
        copyProperties.put(COPIED_DEEP_FIELD, withStreams);
        t = new Content(to, copyProperties);
        update(t);
        LOGGER.debug("Copy Updated {} {} ",to,t);

        for (String stream : streams) {
            InputStream fromStream = null;
            try {
	            fromStream = getInputStream(from, stream);
	            writeBody(to, fromStream);
            } finally {
            	closeSilent(fromStream);
            }
        }
        eventListener.onUpdate(Security.ZONE_CONTENT, to, accessControlManager.getCurrentUserId(), getResourceType(f), true, null, "op:copy");

    }

    public List<ActionRecord> move(String from, String to) throws AccessDeniedException,
        StorageClientException {
      return move(from, to, false);
    }

    public List<ActionRecord> move(String from, String to, boolean force)
        throws AccessDeniedException, StorageClientException {
      List<ActionRecord> record = Lists.newArrayList();

      moveContent(from, to, force);

      PreemptiveIterator<String> iter = (PreemptiveIterator<String>) listChildPaths(from);
      while (iter.hasNext()) {
        String childPath = iter.next();

        // Since this is a direct child of the previous from, only the last token needs to
        // be appended to "to"
        record.addAll(move(childPath, to.concat(childPath.substring(childPath.lastIndexOf("/"))),
        		force));
      }

      record.add(new ActionRecord(from, to));
      return record;
    }
    
    private void moveContent(String from, String to, boolean force) throws AccessDeniedException, StorageClientException {
        // to move, get the structure object out and modify, recreating parent
        // objects as necessary.
        checkOpen();
        accessControlManager.check(Security.ZONE_CONTENT, from, Permissions.CAN_ANYTHING);
        accessControlManager.check(Security.ZONE_CONTENT, to,
                Permissions.CAN_READ.combine(Permissions.CAN_WRITE));
        
        if (!exists(from)) {
            throw new StorageClientException("The source content to move from " + from
                    + " does not exist, move operation failed");
        }
        
        if (exists(to)) {
        	if (force) {
        		delete(to);
        	} else {
        		throw new StorageClientException("The destination content to move to " + to
        			+ " exists, move operation failed");
        	}
        }
        
        // move all files (not directories) from the source to the destination
        File fromFile = fs.getFile(from);
        for (File file : fromFile.listFiles()) {
        	if (!file.isDirectory()) {
        		file.renameTo(fs.getFile(StorageClientUtils.newPath(from, file.getName())));
        	}
        }

        // move the ACLs
        moveAcl(from, to, force);

        delete(from);
        
        // move does not add resourceTypes to events.
        eventListener.onDelete(Security.ZONE_CONTENT, from, accessControlManager.getCurrentUserId(), null, null, "op:move");
        eventListener.onUpdate(Security.ZONE_CONTENT, to, accessControlManager.getCurrentUserId(), null, true, null, "op:move");
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
        
        // simply copy into the next version node
        try {
        	copy(path, nextVersionDirName, true);
        } catch (IOException e) {
        	throw new StorageClientException("Error trying to store content version.", e);
        }
        
        Content savedVersion = get(nextVersionDirName);
        savedVersion.setProperty(READONLY_FIELD, TRUE);
        update(savedVersion);
        return nextVersionNumber;
    }

    public List<String> getVersionHistory(String path) throws AccessDeniedException,
            StorageClientException {
        checkOpen();
        accessControlManager.check(Security.ZONE_CONTENT, path, Permissions.CAN_READ);
        List<Integer> versionHistory = new LinkedList<Integer>();
        if (exists(path)) {
        	VersionFileFilter versionFilter = new VersionFileFilter();
            File target = fs.getFile(path);
            for (File versionFile : target.listFiles(versionFilter)) {
        		// this version file is for the target file name. log it
        		versionHistory.add(versionFilter.getVersionNumber(versionFile.getName()));
            }
            
            // sort the ints and turn it into a string list
            Collections.sort(versionHistory);
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

    public Iterable<Content> find(Map<String, Object> searchProperties) throws StorageClientException,
        AccessDeniedException {
      checkOpen();
      final Map<String, Object> finalSearchProperties = searchProperties;
      return new Iterable<Content>() {

        public Iterator<Content> iterator() {
            Iterator<Content> contentResultsIterator = null;
            try {
              final DisposableIterator<Map<String,Object>> clientSearchKeysIterator = client.find(keySpace, contentColumnFamily, finalSearchProperties, ContentManagerImpl.this);
              contentResultsIterator = new PreemptiveIterator<Content>() {
                  Content contentResult;

                  protected boolean internalHasNext() {
                      contentResult = null;
                      while (contentResult == null && clientSearchKeysIterator.hasNext()) {
                          try {
                              Map<String, Object> structureMap = clientSearchKeysIterator.next();
                              LOGGER.debug("Loaded Next as {} ", structureMap);
                              if ( exists(structureMap) ) {
                                  String path = (String) structureMap.get(PATH_FIELD);
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
                      clientSearchKeysIterator.close();
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
    
    public int count(Map<String, Object> countSearch) throws StorageClientException {
        Builder<String, Object> b = ImmutableMap.builder();
        b.putAll(countSearch);
        b.put(StorageConstants.CUSTOM_STATEMENT_SET, "countestimate");
        b.put(StorageConstants.RAWRESULTS, true);
        DisposableIterator<Map<String,Object>> counts = client.find(keySpace, contentColumnFamily, b.build(), ContentManagerImpl.this);
        try {
            Map<String, Object> count = counts.next();
            return Integer.parseInt(String.valueOf(count.get("1")));
        } finally {
            if ( counts != null ) {
                counts.close();
            }
        }
    }


    public boolean hasBody(String path, String streamId) throws StorageClientException, AccessDeniedException {
        Content content = get(path);
        return client.hasBody(content.getProperties(), streamId);
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

      // check that we have the same permissions as used in ContentManager.move(..)
      accessControlManager.check(Security.ZONE_CONTENT, from, Permissions.CAN_ANYTHING);
      accessControlManager.check(Security.ZONE_CONTENT, to, Permissions.CAN_READ.combine(Permissions.CAN_WRITE));

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
    
    private Map<String, Object> getFileProperties(String path) {
        File directory = fs.getFile(path);
        if (!directory.exists())
        	return null;
        
        File propertiesFile = fs.getFile(StorageClientUtils.newPath(path, FILE_PROPERTIES));
        if (!propertiesFile.exists())
        	return ImmutableMap.of();
        
        ObjectInputStream pin = null;
        try {
        	pin = new ObjectInputStream(fs.getInput(propertiesFile));
        	return (Map<String, Object>)pin.readObject();
        } catch (IOException e) {
        	LOGGER.warn("Received exception trying to load file properties.", e);
        } catch (ClassNotFoundException e) {
        	LOGGER.warn("Received exception trying to load file properties.", e);
		} finally {
        	closeSilent(pin);
        }
        
        return null;
    }
    
    private void putProperties(String path, Map<String, Object> toSave) throws StorageClientException {
        try {
	        File dir = fs.getFile(path);
	        dir.mkdir();
	        
	        File props = fs.getFile(StorageClientUtils.newPath(path, FILE_PROPERTIES));
	        props.createNewFile();
	        
	        ObjectOutputStream os = null;
	        try {
	        	os = new ObjectOutputStream(new FileOutputStream(props));
	        	os.writeObject(toSave);
	        } finally {
	        	closeSilent(os);
	        }
        } catch (IOException e) {
        	throw new StorageClientException(String.format("Error persisting content at '%s'.", path), e);
        }
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

    /**
     * A file filter to help determine what is a file that should be versioned.
     */
    private class VersionableFileFilter implements FileFilter {
		public boolean accept(File pathname) {
			return pathname.isFile() && pathname.getName().startsWith(PREFIX_CONTENT);
		}
    }
    
    /**
     * A file filter to help identify what is a live piece of externally-visible path content,
     * and what is a system/internal file or folder.
     */
    private class LiveContentFileFilter implements FileFilter {
		public boolean accept(File pathname) {
			return pathname.isDirectory() && !pathname.getName().startsWith(PREFIX_CONTENT);
		}
    }
    
    /**
     * A file filter to help identify what is an internal stream file.
     */
    private class StreamFileFilter implements FileFilter {
		
    	/* (non-Javadoc)
    	 * 
    	 * Determines whether or not the given file represents a binary content stream.
    	 * 
    	 * @see java.io.FileFilter#accept(java.io.File)
    	 */
    	public boolean accept(File pathname) {
			return pathname.isFile() && pathname.getName().startsWith(PREFIX_STREAM);
		}
    	
    	/**
    	 * Get the target streamId from the given stream file name.
    	 * 
    	 * @param fileName
    	 * @return
    	 */
    	public String getStreamId(String fileName) {
    		return fileName.replace(PREFIX_STREAM, "");
    	}
    }
    
    /**
     * A utility class that helps identify files that are version stores, and helps parse the
     * required information out of that version file name.
     */
    private class VersionFileFilter implements FileFilter {

    	private final Pattern PATTERN_VERSION = Pattern.compile(String.format(
    			"%s:([0-9]+)", PREFIX_VERSION));
    			
		/* (non-Javadoc)
		 * 
		 * Determine whether or not the given file is a version store for another file.
		 *  
		 * @see java.io.FileFilter#accept(java.io.File)
		 */
		public boolean accept(File pathname) {
			return PATTERN_VERSION.matcher(pathname.getName()).matches();
		}
		
		/**
		 * Get the version number the given filename represents.
		 * 
		 * @param fileName
		 * @return
		 */
		public Integer getVersionNumber(String fileName) {
			return Integer.valueOf(PATTERN_VERSION.matcher(fileName).group(1));
		}
    }
}
