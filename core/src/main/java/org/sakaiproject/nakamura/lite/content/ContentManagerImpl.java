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
import org.infinispan.context.Flag;
import org.infinispan.io.GridFile.Metadata;
import org.infinispan.io.GridFilesystem;
import org.infinispan.manager.CacheContainer;
import org.infinispan.query.QueryIterator;
import org.infinispan.tree.Fqn;
import org.infinispan.tree.Node;
import org.infinispan.tree.TreeCache;
import org.infinispan.tree.TreeCacheFactory;
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
import org.sakaiproject.nakamura.lite.content.io.GridFileInputStreamWrapper;
import org.sakaiproject.nakamura.lite.storage.spi.StorageClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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
 *
 */
public class ContentManagerImpl implements ContentManager {

  private static Logger LOGGER = LoggerFactory.getLogger(ContentManagerImpl.class);
  
  private static Fqn CONTENT_FQN = Fqn.fromRelativeElements(Fqn.root(), "oae#content");
  
  // filesystem and stream constants
  private static final String FILESYSTEM_ROOT = "/.oae/";
  private static final String PREFIX_STREAM = "oae:body";
  private static final String FILE_STREAM_DEFAULT = PREFIX_STREAM;
  
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
  
  protected TreeCache<String, Object> treeCache;

  protected GridFilesystem fs;
  
  protected FilesystemHelper fsHelper;
  
  protected StorageClient client;

  private AccessControlManager accessControlManager;

  private boolean closed;

  private StoreListener eventListener;

  private PathPrincipalTokenResolver pathPrincipalResolver;
  
  public ContentManagerImpl(CacheContainer cacheContainer, StorageClient client,
      AccessControlManager accessControlManager, Configuration config,
      Map<String, CacheHolder> sharedCache, StoreListener eventListener) {
    this.client = client;
    closed = false;
    this.eventListener = eventListener;
    String userId = accessControlManager.getCurrentUserId();
    String usersTokenPath = StorageClientUtils.newPath(userId, "private/tokens");
    this.pathPrincipalResolver = new PathPrincipalTokenResolver(usersTokenPath, this);
    this.accessControlManager = new AccessControlManagerTokenWrapper(
        accessControlManager, pathPrincipalResolver);
    
    Cache<String, Object> contentCache = cacheContainer.<String, Object>getCache(config
        .getContentPropertiesCacheName());
    Cache<String, Metadata> metadataCache = cacheContainer.<String, Metadata>getCache(
        config.getContentMetadataName());
    Cache<String, byte[]> bodyCache = cacheContainer.<String, byte[]>getCache(
        config.getContentBodyCacheName());
    
    treeCache = (new TreeCacheFactory()).createTreeCache(contentCache);
    fs = new GridFilesystem(bodyCache, metadataCache);
    fsHelper = new FilesystemHelper(fs, metadataCache);
    
  }
  
  /**
   * {@inheritDoc}
   * @see org.sakaiproject.nakamura.api.lite.content.ContentManager#get(java.lang.String)
   */
  public Content get(String path) throws StorageClientException {
    checkOpen();
    try {
      accessControlManager.check(Security.ZONE_CONTENT, path, Permissions.CAN_READ);
      Map<String, Object> props = treeCache.getData(getContentFqn(path));
      if (props != null) {
        Content content = new Content(path, props);
        ((InternalContent) content).internalize(this, false);
        return content;
      }
    } catch (AccessDeniedException e) {
      LOGGER.debug("Access denied acquiring content.", e);
    }
    return null;
  }

  /**
   * {@inheritDoc}
   * 
   * @see org.sakaiproject.nakamura.api.lite.content.ContentManager#find(org.apache.lucene.search.Query, org.apache.lucene.search.Sort)
   */
  public Iterable<Content> find(final Query luceneQuery, final Sort sort)
      throws StorageClientException, AccessDeniedException {
    checkOpen();
    return new Iterable<Content>() {
      public Iterator<Content> iterator() {
        Iterator<Content> contentResultsIterator = null;
        try {
          final QueryIterator documents = client.find(luceneQuery, sort);

          contentResultsIterator = new PreemptiveIterator<Content>() {
            private Content contentResult;

            protected boolean internalHasNext() {
              contentResult = null;
              while (contentResult == null && documents.hasNext()) {
                try {
                  IndexDocument document = (IndexDocument) documents.next();
                  LOGGER.debug("Loaded Next as {} ", document);
                  if (exists(document.getId())) {
                    String path = document.getId();
                    contentResult = get(path);
                  }
                } catch (StorageClientException e) {
                  LOGGER.debug(e.getMessage(), e);
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

  /**
   * {@inheritDoc}
   * @see org.sakaiproject.nakamura.api.lite.content.ContentManager#count(org.apache.lucene.search.Query)
   */
  public int count(Query query) throws StorageClientException {
    return client.count(query);
  }

  /**
   * {@inheritDoc}
   * @see org.sakaiproject.nakamura.api.lite.content.ContentManager#saveVersion(java.lang.String)
   */
  public String saveVersion(String path) throws StorageClientException,
      AccessDeniedException {
    return saveVersion(path, null);
  }

  /**
   * {@inheritDoc}
   * @see org.sakaiproject.nakamura.api.lite.content.ContentManager#saveVersion(java.lang.String, java.util.Map)
   */
  public String saveVersion(String path, Map<String, Object> versionMetadata)
      throws StorageClientException, AccessDeniedException {
    checkOpen();
    accessControlManager.check(Security.ZONE_CONTENT, path, Permissions.CAN_WRITE);
    if (!exists(path)) {
        throw new StorageClientException("Item "+path+" does not exist");
    }
  
    // get the next version
    List<String> versionHistory = getVersionHistory(path);
    int nextVersionNumber = versionHistory.size()+1;
    String nextVersionNodeName = VersionHelper.getVersionNodeName(nextVersionNumber);
    
    Node<String, Object> toVersion = treeCache.getNode(getContentFqn(path));
    Node<String, Object> versionNode = toVersion.addChild(Fqn.fromString(nextVersionNodeName));
    versionNode.putAll(toVersion.getData());
    
    if (versionMetadata != null) {
      for (String metaKey : versionMetadata.keySet()) {
        versionNode.put(String.format("metadata:%s", metaKey), versionMetadata.get(metaKey));
      }
    }
  
    return String.valueOf(nextVersionNumber);
  }

  /**
   * {@inheritDoc}
   * @see org.sakaiproject.nakamura.api.lite.content.ContentManager#update(org.sakaiproject.nakamura.api.lite.content.Content)
   */
  public void update(Content content) throws AccessDeniedException,
      StorageClientException {
    update(content, true);
  }

  /**
   * {@inheritDoc}
   * @see org.sakaiproject.nakamura.api.lite.content.ContentManager#update(org.sakaiproject.nakamura.api.lite.content.Content, boolean)
   */
  public void update(Content content, boolean withTouch) throws AccessDeniedException,
      StorageClientException {
    checkOpen();
    String path = content.getPath();
    
    // we need to both read and write content to perform an update
    accessControlManager.check(Security.ZONE_CONTENT, content.getPath(),
        Permissions.CAN_WRITE.combine(Permissions.CAN_READ));
    
    // ensure isolation
    treeCache.getCache().startBatch();
    
    Map<String, Object> originalProperties = ImmutableMap.of();
    Map<String, Object> toSave = null;
    
    boolean isnew = content.isNew();
    
    try {

      // a concurrency issue has resulted in attempting to update something that was
      // already deleted. Simply leave the element as deleted.
      if (!isnew && !exists(path))
        return;
      
      boolean touch = withTouch || !User.ADMIN_USER.equals(accessControlManager.getCurrentUserId());
      
      if (isnew) {
        // create the parents if necessary
        if (!StorageClientUtils.isRoot(path)) {
          String parentPath = StorageClientUtils.getParentObjectPath(path);
          Content parentContent = get(parentPath);
          if (parentContent == null) {
            update(new Content(parentPath, null), withTouch);
          }
        } else {
          // ensure the root content directory is seeded
          if (!treeCache.exists(CONTENT_FQN))
            treeCache.put(CONTENT_FQN, new HashMap<String, Object>());
        }
        
        toSave = Maps.newHashMap(content.getPropertiesForUpdate());
        toSave.put(PATH_FIELD, path);
        
        if (touch) {
          // if the user is admin we allow overwriting of protected fields. This should allow content migration.
          toSave.put(CREATED_FIELD, System.currentTimeMillis());
          toSave.put(CREATED_BY_FIELD, accessControlManager.getCurrentUserId());
          toSave.put(LASTMODIFIED_FIELD, System.currentTimeMillis());
          toSave.put(LASTMODIFIED_BY_FIELD, accessControlManager.getCurrentUserId());
        }
        
        LOGGER.debug("New Content with {} {} ", path, toSave);
      } else if (content.isUpdated()) {
        originalProperties = content.getOriginalProperties();
        
        toSave = new HashMap<String, Object>();
        toSave.putAll(originalProperties);
        toSave.putAll(content.getPropertiesForUpdate());
        for (String protectedKey : PROTECTED_FIELDS) {
          toSave.put(protectedKey, originalProperties.get(protectedKey));
        }
      } else {
        return;
      }
      
      if (exists(path)) {
        Node<String, Object> originalNode = treeCache.getNode(getContentFqn(path));
        if (TRUE.equals(originalNode.get(READONLY_FIELD))) {
          throw new AccessDeniedException(Security.ZONE_CONTENT, path,
              "update on read only Content Item (possibly a previous version of the item)",
              accessControlManager.getCurrentUserId());
        }
      }
      
      // clean null values
      List<String> toRemove = new LinkedList<String>();
      for (String key : toSave.keySet()) {
        if (toSave.get(key) == null) {
          toRemove.add(key);
        }
      }
      
      for (String key : toRemove) {
        toSave.remove(key);
      }
      
      // finally save
      treeCache.put(getContentFqn(path), toSave);
      
      // update the content index
      if (!isnew) {
        client.removeIndex(path, originalProperties);
      }
      client.updateIndex(path, toSave);
      
      // reset state to unmodified to take further modifications.
      content.reset(toSave);
      
      eventListener.onUpdate(Security.ZONE_CONTENT, path, accessControlManager.getCurrentUserId(),
          getResourceType(content),  isnew, originalProperties, "op:update");
      
    } finally {
      treeCache.getCache().endBatch(true);
    }
  }

  /**
   * {@inheritDoc}
   * @see org.sakaiproject.nakamura.api.lite.content.ContentManager#delete(java.lang.String)
   */
  public void delete(String path) throws AccessDeniedException, StorageClientException {
    delete(path, false);
  }

  /**
   * {@inheritDoc}
   * @see org.sakaiproject.nakamura.api.lite.content.ContentManager#delete(java.lang.String, boolean)
   */
  public void delete(String path, boolean recurse) throws AccessDeniedException,
      StorageClientException {
    checkOpen();
    checkCanDelete(path);
    if (exists(path)) {
      Iterator<String> children = listChildPaths(path);
      if (!recurse && children.hasNext()) {
        throw new StorageClientException("Unable to delete a path with active children ["
            + path + "]. Set recurse=true to delete a tree.");
      }
      
      Node<String, Object> node = treeCache.getNode(getContentFqn(path), Flag.FORCE_WRITE_LOCK);
      Map<String, Object> properties = node.getData();
      treeCache.removeNode(getContentFqn(path));
      
      eventListener.onDelete(Security.ZONE_CONTENT, path, accessControlManager.getCurrentUserId(),
          (String)properties.get(Content.SLING_RESOURCE_TYPE_FIELD), properties);
    }
    
    // TODO: Delete the streams
  }

  /**
   * {@inheritDoc}
   * @see org.sakaiproject.nakamura.api.lite.content.ContentManager#writeBody(java.lang.String, java.io.InputStream)
   */
  public long writeBody(String path, InputStream in) throws StorageClientException,
      AccessDeniedException, IOException {
    return writeBody(path, in, null);
  }

  /**
   * {@inheritDoc}
   * @see org.sakaiproject.nakamura.api.lite.content.ContentManager#getInputStream(java.lang.String)
   */
  public InputStream getInputStream(String path) throws StorageClientException,
      AccessDeniedException, IOException {
    return getInputStream(path, null);
  }

  /**
   * {@inheritDoc}
   * @see org.sakaiproject.nakamura.api.lite.content.ContentManager#writeBody(java.lang.String, java.io.InputStream, java.lang.String)
   */
  public long writeBody(String path, InputStream in, String streamId)
      throws StorageClientException, AccessDeniedException, IOException {
    checkOpen();
    accessControlManager.check(Security.ZONE_CONTENT, path, Permissions.CAN_WRITE);
    
    if (!exists(path)) {
      update(new Content(path, null));
    }
    
    File parentFile = getFileFromContentPath(path);
    parentFile.mkdirs();
    
    File streamFile = getFileFromContentPath(StorageClientUtils.newPath(path,
        getStreamFileNameByStreamId(streamId)));
    
    streamFile.createNewFile();
    
    // download the file onto the file-system
    OutputStream os = null;
    try {
      os = fs.getOutput(streamFile.getAbsolutePath());
      IOUtils.copyLarge(in, os);
    } finally {
      IOUtils.closeQuietly(os);
    }
    
    Content content = get(path);
    
    if (streamId != null) {
      @SuppressWarnings("unchecked")
      Set<String> streams = (Set<String>) content.getProperty(STREAMS_FIELD);
      if (streams == null) {
        streams = new HashSet<String>();
        streams.add(streamId);
        content.setProperty(STREAMS_FIELD, streams);
      } else if (!streams.contains(streamId)){
        streams.add(streamId);
        content.setProperty(STREAMS_FIELD, streams);
      }
    }
    
    update(content, true);
    eventListener.onUpdate(Security.ZONE_CONTENT, path, accessControlManager.getCurrentUserId(),
        getResourceType(content), false, null, "stream", streamId);
    return streamFile.length();
  }

  /**
   * {@inheritDoc}
   * @see org.sakaiproject.nakamura.api.lite.content.ContentManager#getInputStream(java.lang.String, java.lang.String)
   */
  public InputStream getInputStream(String path, String streamId)
      throws StorageClientException, AccessDeniedException, IOException {
    checkOpen();
    accessControlManager.check(Security.ZONE_CONTENT, path, Permissions.CAN_READ);
    String streamPath = StorageClientUtils.newPath(path, getStreamFileNameByStreamId(streamId));
    File streamFile = getFileFromContentPath(streamPath);
    if (!streamFile.exists())
      return new ByteArrayInputStream(new byte[0]);
    
    return new BufferedInputStream(new GridFileInputStreamWrapper(
        fs.getInput(streamFile)));
  }

  /**
   * {@inheritDoc}
   * @throws AccessDeniedException 
   * @see org.sakaiproject.nakamura.api.lite.content.ContentManager#exists(java.lang.String)
   */
  public boolean exists(String path) {
    try {
      checkOpen();
      accessControlManager.check(Security.ZONE_CONTENT, path, Permissions.CAN_READ);
      return treeCache.exists(getContentFqn(path));
    } catch (StorageClientException e) {
      LOGGER.warn("Error checking if content exists.", e);
    } catch (AccessDeniedException e) {
      LOGGER.debug(e.getMessage(), e);
    }
    return false;
  }

  /**
   * {@inheritDoc}
   * @see org.sakaiproject.nakamura.api.lite.content.ContentManager#copy(java.lang.String, java.lang.String, boolean)
   */
  public void copy(String from, String to, boolean withStreams)
      throws StorageClientException, AccessDeniedException, IOException {
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
      LOGGER.debug("Deleting {} ", to);
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
    LOGGER.debug("Copy Updated {} {} ", to, t);

    if (withStreams) {

      // first copy the default stream if it exists
      InputStream defaultStream = null;
      try {
        defaultStream = getInputStream(from, null);
        if (defaultStream != null) {
          writeBody(to, defaultStream);
        }
      } finally {
        IOUtils.closeQuietly(defaultStream);
      }

      for (String stream : streams) {
        InputStream fromStream = null;
        try {
          fromStream = getInputStream(from, stream);
          writeBody(to, fromStream, stream);
        } finally {
          IOUtils.closeQuietly(fromStream);
        }
      }
    }
  }

  /**
   * {@inheritDoc}
   * @see org.sakaiproject.nakamura.api.lite.content.ContentManager#move(java.lang.String, java.lang.String)
   */
  public List<ActionRecord> move(String from, String to) throws AccessDeniedException,
      StorageClientException {
    return move(from, to, false);
  }

  /**
   * {@inheritDoc}
   * @see org.sakaiproject.nakamura.api.lite.content.ContentManager#move(java.lang.String, java.lang.String, boolean)
   */
  public List<ActionRecord> move(String from, String to, boolean force)
      throws AccessDeniedException, StorageClientException {
    return move(from, to, force, true);
  }

  /**
   * {@inheritDoc}
   * @see org.sakaiproject.nakamura.api.lite.content.ContentManager#move(java.lang.String, java.lang.String, boolean, boolean)
   */
  public List<ActionRecord> move(String from, String to, boolean force,
      boolean keepDestHistory) throws AccessDeniedException,
      StorageClientException {
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
    
    // handle the node properties
    Node<String, Object> fromNode = treeCache.getNode(getContentFqn(from));
    Fqn toFqn = getContentFqn(to);
    if (treeCache.exists(toFqn)) {
      Node<String, Object> toNode = treeCache.getNode(toFqn);
      toNode.clearData();
      toNode.putAll(fromNode.getData());
    } else {
      treeCache.put(toFqn, fromNode.getData());
    }
    
    // delete the source
    treeCache.removeNode(getContentFqn(from));
    
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

  /**
   * {@inheritDoc}
   * @see org.sakaiproject.nakamura.api.lite.content.ContentManager#getVersion(java.lang.String, java.lang.String)
   */
  public Content getVersion(String path, String versionId) throws StorageClientException,
      AccessDeniedException {
    checkOpen();
    accessControlManager.check(Security.ZONE_CONTENT, path, Permissions.CAN_READ);
    if (exists(path)) {
      try {
        String versionNodeName = VersionHelper.getVersionNodeName(Integer.valueOf(versionId));
        Fqn contentFqn = getContentFqn(path);
        Fqn versionFqn = Fqn.fromRelativeElements(contentFqn, versionNodeName);
        return new Content(path, treeCache.getData(versionFqn));
      } catch (NumberFormatException e) {
        LOGGER.warn("Tried to access invalid version number: {}. This should be an integer.", versionId);
      }
    }
    return null;
  }

  /**
   * {@inheritDoc}
   * @see org.sakaiproject.nakamura.api.lite.content.ContentManager#getVersionInputStream(java.lang.String, java.lang.String, java.lang.String)
   */
  public InputStream getVersionInputStream(String path, String versionId, String streamId)
      throws AccessDeniedException, StorageClientException, IOException {
    // TODO Auto-generated method stub
    return null;
  }

  /**
   * {@inheritDoc}
   * @see org.sakaiproject.nakamura.api.lite.content.ContentManager#getVersionInputStream(java.lang.String, java.lang.String)
   */
  public InputStream getVersionInputStream(String path, String versionId)
      throws AccessDeniedException, StorageClientException, IOException {
    // TODO Auto-generated method stub
    return null;
  }

  /**
   * {@inheritDoc}
   * @see org.sakaiproject.nakamura.api.lite.content.ContentManager#getVersionHistory(java.lang.String)
   */
  public List<String> getVersionHistory(String path) throws AccessDeniedException,
      StorageClientException {
    checkOpen();
    accessControlManager.check(Security.ZONE_CONTENT, path, Permissions.CAN_READ);
    
    List<Integer> versionHistory = new LinkedList<Integer>();
    List<String> versionHistoryStr = new LinkedList<String>();
    
    if (exists(path)) {
      Fqn contentFqn = getContentFqn(path);
      Node<String, Object> toVersion = treeCache.getNode(contentFqn);
      Set<Object> childrenNames = toVersion.getChildrenNames();
      for (Object nameObj : childrenNames) {
        String name = getNodeName(nameObj);
        if (VersionHelper.isVersionNode(name)) {
          versionHistory.add(VersionHelper.getVersionNumberFromNodeName(name));
        }
      }
      
      // sort descending then convert to strings
      Collections.sort(versionHistory, Collections.reverseOrder());
      for (Integer num : versionHistory) {
        versionHistoryStr.add(num.toString());
      }
    }
    
    return versionHistoryStr;
  }

  /**
   * {@inheritDoc}
   * 
   * @see org.sakaiproject.nakamura.api.lite.content.ContentManager#listChildPaths(java.lang.String)
   */
  public Iterator<String> listChildPaths(final String path) throws StorageClientException {
    if (!exists(path))
      return (new LinkedList<String>()).iterator();

    Node<String, Object> contentNode = treeCache.getNode(getContentFqn(path));
    Set<Object> childrenNames = contentNode.getChildrenNames();

    if (childrenNames.isEmpty())
      return (new LinkedList<String>()).iterator();

    final Iterator<Object> children = childrenNames.iterator();
    return new PreemptiveIterator<String>() {
      private String nextPath;

      @Override
      protected boolean internalHasNext() {
        nextPath = null;
        while (nextPath == null && children.hasNext()) {
          String contentPath = StorageClientUtils.newPath(path, children.next().toString());
          if (isLiveContent(contentPath) && exists(contentPath)) {
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

  /**
   * {@inheritDoc}
   * 
   * @see org.sakaiproject.nakamura.api.lite.content.ContentManager#listChildren(java.lang.String)
   */
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
          LOGGER.warn("Error getting the next child of " + path, e);
        }

        return false;
      }

      @Override
      protected Content internalNext() {
        return content;
      }
    };
  }

  /**
   * {@inheritDoc}
   * @see org.sakaiproject.nakamura.api.lite.content.ContentManager#hasBody(java.lang.String, java.lang.String)
   */
  public boolean hasBody(String path, String streamId) throws StorageClientException,
      AccessDeniedException {
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
   * {@inheritDoc}
   * 
   * @see org.sakaiproject.nakamura.api.lite.content.ContentManager#triggerRefresh(java.lang.String)
   */
  public void triggerRefresh(String path) throws StorageClientException,
      AccessDeniedException {
    Content c = get(path);
    if (c != null) {
      eventListener.onUpdate(Security.ZONE_CONTENT, path,
          accessControlManager.getCurrentUserId(), getResourceType(c), false, null,
          "op:update");
    }
  }

  /**
   * {@inheritDoc}
   * @see org.sakaiproject.nakamura.api.lite.content.ContentManager#triggerRefreshAll()
   */
  public void triggerRefreshAll() throws StorageClientException {
    triggerRefreshAll(CONTENT_FQN);
  }
  
  private void triggerRefreshAll(Fqn parentFqn) throws StorageClientException {
    if (User.ADMIN_USER.equals(accessControlManager.getCurrentUserId()) ) {
      String path = parentFqn.toString().substring(CONTENT_FQN.toString().length());
      
      // refresh the current node
      try {
        triggerRefresh(path);
      } catch (AccessDeniedException e) {
        LOGGER.error("Exception while refreshing all content", e);
      }
      
      // refresh all children
      Node<String, Object> parentNode = treeCache.getNode(parentFqn);
      for (Object childName : parentNode.getChildrenNames()) {
        if (!VersionHelper.isVersionNode(childName.toString())) {
          Node<String, Object> child = parentNode.getChild(childName);
          triggerRefreshAll(child.getFqn());
        }
      }
    }
  }

  /**
   * {@inheritDoc}
   * @see org.sakaiproject.nakamura.api.lite.content.ContentManager#replace(org.sakaiproject.nakamura.api.lite.content.Content)
   */
  public void replace(Content content) throws AccessDeniedException,
      StorageClientException {
    replace(content, true);
  }

  /**
   * {@inheritDoc}
   * @see org.sakaiproject.nakamura.api.lite.content.ContentManager#replace(org.sakaiproject.nakamura.api.lite.content.Content, boolean)
   */
  public void replace(Content content, boolean withTouch) throws AccessDeniedException,
      StorageClientException {
    Content current = get(content.getPath());
    if (current != null) {
      Set<String> diffKeys = diffKeys(current.getProperties(), content.getProperties());
      for (String diffKey : diffKeys) {
        content.setProperty(diffKey, new RemoveProperty());
      }
    }
    update(content, withTouch);
  }

  public void close() {
    closed = true;
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

  /**
   * Permanently delete the streams that are associated with the content at the given
   * content path.
   * 
   * @param path
   * @throws AccessDeniedException
   * @throws StorageClientException
   */
  private void hardDeleteStreams(String path) throws StorageClientException,
      AccessDeniedException {
    
    // delete the named streams
    Node<String, Object> contentNode = treeCache.getNode(getContentFqn(path));
    if (contentNode != null) {
      Map<String, Object> properties = contentNode.getData();
      if (properties != null) {
        @SuppressWarnings("unchecked")
        Set<String> streams = (Set<String>) properties.get(STREAMS_FIELD);
        if (streams != null) {
          for (String streamId : streams) {
            File streamFile = fs.getFile(getFilesystemPath(path), getStreamFileNameByStreamId(
                streamId));
            if (streamFile.exists()) {
              streamFile.delete();
            }
            streams.remove(streamId);
          }
        }
        contentNode.remove(STREAMS_FIELD);
      }
    }

    // delete the "default" stream
    File streamFile = fs.getFile(getFilesystemPath(path), PREFIX_STREAM);
    if (streamFile.exists())
      streamFile.delete();
  }

  /**
   * Replace the streams at the {@code to} content path with those at the {@code from}
   * content path.
   * 
   * @param from
   * @param to
   * @throws IOException
   * @throws AccessDeniedException
   * @throws StorageClientException
   */
  private void replaceStreams(String from, String to) throws IOException,
      StorageClientException, AccessDeniedException {
    hardDeleteStreams(to);

    // replace the named streams
    Node<String, Object> fromContentNode = treeCache.getNode(getContentFqn(from));
    Map<String, Object> fromProps = fromContentNode.getData();
    if (fromProps != null) {
      @SuppressWarnings("unchecked")
      Set<String> streams = (Set<String>) fromProps.get(STREAMS_FIELD);
      if (streams != null) {
        for (String streamId : streams) {
          String fromStreamPath = StorageClientUtils.newPath(getFilesystemPath(from),
              streamId);
          String toStreamPath = StorageClientUtils.newPath(getFilesystemPath(to),
              streamId);
          fsHelper.copyFile(fromStreamPath, toStreamPath);
        }
        
        Node<String, Object> toContentNode = treeCache.getNode(getContentFqn(to));
        toContentNode.put(STREAMS_FIELD, streams);
      }
    }

    // replace the default stream
    File fromStream = fs.getFile(getFilesystemPath(from), PREFIX_STREAM);
    if (fromStream.exists()) {
      String toStreamPath = StorageClientUtils.newPath(getFilesystemPath(to),
          PREFIX_STREAM);
      fsHelper.copyFile(fromStream.getAbsolutePath(), toStreamPath);
    }
  }

  /**
   * Permanently delete the versions from the file-system that are associated with the
   * content at the given content path.
   * 
   * @param path
   * @throws StorageClientException 
   * @throws AccessDeniedException 
   * @throws NumberFormatException 
   */
  private void hardDeleteVersions(String path) throws NumberFormatException, AccessDeniedException, StorageClientException {
    Fqn contentFqn = getContentFqn(path);
    // loop through possible version names until we find one that doesn't exist.
    for (String versionNumber : getVersionHistory(path)) {
      Fqn versionFqn = Fqn.fromRelativeElements(contentFqn, VersionHelper.getVersionNodeName(
          Integer.valueOf(versionNumber)));
      treeCache.removeNode(versionFqn);
    }
  }

  /**
   * Replace the versions at the {@code to} content path with those at the {@code from}
   * content path.
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
    Fqn fromFqn = getContentFqn(from);
    Fqn toFqn = getContentFqn(to);
    for (String versionNumber : getVersionHistory(from)) {
      String versionNodeName = VersionHelper.getVersionNodeName(Integer.valueOf(versionNumber));
      treeCache.move(Fqn.fromRelativeElements(fromFqn, versionNodeName),
          Fqn.fromRelativeElements(toFqn, versionNodeName));
    }
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
  

  private File getFileFromContentPath(String contentPath) {
    return fs.getFile(getFilesystemPath(contentPath));
  }
  
  private String getFilesystemPath(String contentPath) {
    String filesystemPath = contentPath;
    if (!contentPath.startsWith(FILESYSTEM_ROOT)) {
      filesystemPath = StorageClientUtils.newPath(FILESYSTEM_ROOT, contentPath);
    }
    return filesystemPath;
  }
  
  private boolean isLiveContent(String path) {
    Fqn fqn = Fqn.fromString(path);
    String name = fqn.getLastElementAsString();
    return !VersionHelper.isVersionNode(name);
  }
  
  private String getStreamFileNameByStreamId(String streamId) {
    if (StringUtils.isEmpty(streamId))
      return FILE_STREAM_DEFAULT;
    return String.format("%s:%s", PREFIX_STREAM, streamId);
  }

  private String getNodeName(Object nodeNameObj) {
    return (nodeNameObj != null) ? nodeNameObj.toString() : null;
  }
  
  private Fqn getContentFqn(String path) {
    List<String> parts = new LinkedList<String>();
    for (String part : path.split("/")) {
      if (StringUtils.isNotBlank(part)) {
        parts.add(part);
      }
    }
    return Fqn.fromRelativeElements(CONTENT_FQN, parts.toArray(new Object[parts.size()]));
  }
  
  private void checkOpen() throws StorageClientException {
    if (closed) {
      throw new StorageClientException("Content Manager is closed");
    }
  }
}
