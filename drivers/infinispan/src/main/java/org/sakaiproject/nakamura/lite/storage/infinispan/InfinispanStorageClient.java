/**
 * 
 */
package org.sakaiproject.nakamura.lite.storage.infinispan;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.infinispan.Cache;
import org.sakaiproject.nakamura.api.lite.RemoveProperty;
import org.sakaiproject.nakamura.api.lite.StorageClientException;
import org.sakaiproject.nakamura.api.lite.StorageClientUtils;
import org.sakaiproject.nakamura.api.lite.accesscontrol.AccessDeniedException;
import org.sakaiproject.nakamura.api.lite.content.Content;
import org.sakaiproject.nakamura.api.lite.util.PreemptiveIterator;
import org.sakaiproject.nakamura.lite.storage.spi.DirectCacheAccess;
import org.sakaiproject.nakamura.lite.storage.spi.Disposable;
import org.sakaiproject.nakamura.lite.storage.spi.DisposableIterator;
import org.sakaiproject.nakamura.lite.storage.spi.Disposer;
import org.sakaiproject.nakamura.lite.storage.spi.SparseMapRow;
import org.sakaiproject.nakamura.lite.storage.spi.SparseRow;
import org.sakaiproject.nakamura.lite.storage.spi.StorageClient;
import org.sakaiproject.nakamura.lite.storage.spi.StorageClientListener;
import org.sakaiproject.nakamura.lite.storage.spi.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * @author Branden
 * 
 */
public class InfinispanStorageClient implements StorageClient {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(InfinispanStorageClient.class);
	private static final String INDEX_COLUMN_FAMILY = "smcindex";

	private static final Set<String> INTERNAL_INDEX_COLUMNS = ImmutableSet.<String>of(
			Content.PARENT_HASH_FIELD);
	
	private final Cache<String, Object> storage;

	private Set<String> indexColumns;
	private StorageClientListener storageClientListener;
	private List<Disposable> toDispose = Lists.newArrayList();
	public List<Map<String, Object>> tResultRows;

	public InfinispanStorageClient(Cache<String, Object> storage, Set<String> indexColumns) {
		this.storage = storage;
		this.indexColumns = indexColumns;
	}

	public Map<String, Object> get(String keySpace, String columnFamily,
			String key) throws StorageClientException {
		String storageKey = createStorageKey(keySpace, columnFamily, key);
		return getMapFromStorage(storageKey);
	}

	public void insert(String keySpace, String columnFamily, String key,
			Map<String, Object> values, boolean probablyNew)
			throws StorageClientException {
		String storageKey = createStorageKey(keySpace, columnFamily, key);

		Map<String, Object> before = get(keySpace, columnFamily, key);
		Map<String, Object> mutableValues = new HashMap<String, Object>(before);

		for (Map.Entry<String, Object> entry : values.entrySet()) {
			String columnName = entry.getKey();
			if (entry.getValue() instanceof RemoveProperty) {
				mutableValues.remove(columnName);
			} else {
				mutableValues.put(columnName, entry.getValue());
			}
			
			// some custom indexing, "borrowed" from the CassandraClient. maybe with more research
			// and work this could be replaced by an internal lucene index.
			if(!columnFamily.equals(INDEX_COLUMN_FAMILY) && shouldIndex(keySpace,
					columnFamily, columnName)) {
				try {
					if (entry.getValue() instanceof RemoveProperty) {
						Object beforeValue = before.get(columnName);
						if (beforeValue != null) {
							byte[] b = Types.toByteArray(beforeValue);
							removeIndex(keySpace, columnFamily, key, columnName, b);
						}
					} else {
						Object value = entry.getValue();
						if (value != null) {
							byte[] b = Types.toByteArray(value);
							addIndex(keySpace, columnFamily, key, columnName, b);
						}
					}
				} catch (IOException e) {
					LOGGER.warn("Was not able to index property '{}' of content '{}'",
							columnName, key);
				}
	        }
		}

		if (storageClientListener != null) {
			storageClientListener.before(keySpace, columnFamily, key, before);
		}
		
		storage.put(storageKey, mutableValues);

		if (storageClientListener != null) {
			storageClientListener.after(keySpace, columnFamily, key,
					mutableValues);
		}
	}

	public void remove(String keySpace, String columnFamily, String key)
			throws StorageClientException {
		String storageKey = createStorageKey(keySpace, columnFamily, key);
		Map<String, Object> values = get(keySpace, columnFamily, key);
		if (values != null) {
			for (Map.Entry<String, Object> entry : values.entrySet()) {
				String name = entry.getKey();
				if (shouldIndex(keySpace, columnFamily, name)) {
					Object value = entry.getValue();
					if (value != null) {
						try {
							byte[] b = Types.toByteArray(entry.getValue());
							removeIndex(keySpace, columnFamily, key, name, b);
						} catch (IOException e) {
							LOGGER.warn("Failed to evict index record for '{}' property '{}'", key, name);
						}
					}
				}
			}
			storage.remove(storageKey);
		}
	}

	public InputStream streamBodyOut(String keySpace, String columnFamily,
			String contentId, String contentBlockId, String streamId,
			Map<String, Object> content) throws StorageClientException,
			AccessDeniedException, IOException {
		return null;
	}

	public Map<String, Object> streamBodyIn(String keySpace,
			String columnFamily, String contentId, String contentBlockId,
			String streamId, Map<String, Object> content, InputStream in)
			throws StorageClientException, AccessDeniedException, IOException {
		return null;
	}

	public DisposableIterator<Map<String, Object>> find(String keySpace,
			String authorizableColumnFamily, Map<String, Object> properties,
			DirectCacheAccess cachingManager) throws StorageClientException {
		final String fKeyspace = keySpace;
		final String fAuthorizableColumnFamily = authorizableColumnFamily;
		List<Set<String>> andTerms = new ArrayList<Set<String>>();

		for (Entry<String, Object> e : properties.entrySet()) {
			String k = e.getKey();
			Object v = e.getValue();

			if (shouldIndex(keySpace, authorizableColumnFamily, k)
					|| (v instanceof Map)) {
				if (v != null) {
					if (v instanceof Map) {
						List<Set<String>> orTerms = new ArrayList<Set<String>>();
						Set<String> orResultSet = new HashSet<String>();

						@SuppressWarnings("unchecked")
						Set<Entry<String, Object>> subterms = ((Map<String, Object>) v)
								.entrySet();

						for (Iterator<Entry<String, Object>> subtermsIter = subterms
								.iterator(); subtermsIter.hasNext();) {
							Entry<String, Object> subterm = subtermsIter.next();
							String subk = subterm.getKey();
							Object subv = subterm.getValue();
							if (shouldIndex(keySpace, authorizableColumnFamily,
									subk)) {
								try {
									Set<String> or = new HashSet<String>();
									String indexKey = new String(
											subk.getBytes("UTF-8"))
											+ ":"
											+ authorizableColumnFamily
											+ ":"
											+ StorageClientUtils
													.insecureHash(new String(
															Types.toByteArray(subv)));
									Map<String, Object> tempRow = get(keySpace,
											INDEX_COLUMN_FAMILY, indexKey);
									for (Entry<String, Object> tempRows : tempRow
											.entrySet()) {
										or.add(tempRows.getKey());
									}
									orTerms.add(or);
								} catch (IOException e1) {
									LOGGER.warn("IOException {}",
											e1.getMessage());
								}
							}
						}

						if (!orTerms.isEmpty())
							orResultSet = orTerms.get(0);

						for (int i = 0; i < orTerms.size(); i++) {
							orResultSet = Sets.union(orResultSet,
									orTerms.get(i));

						}
						andTerms.add(orResultSet);
					} else {
						try {
							Set<String> and = new HashSet<String>();
							String indexKey = new String(k.getBytes("UTF-8"))
									+ ":"
									+ authorizableColumnFamily
									+ ":"
									+ StorageClientUtils
											.insecureHash(new String(Types
													.toByteArray(v)));
							Map<String, Object> tempRow = get(keySpace,
									INDEX_COLUMN_FAMILY, indexKey);
							for (Entry<String, Object> tempRows : tempRow
									.entrySet()) {
								and.add(tempRows.getKey());
							}
							andTerms.add(and);
						} catch (IOException e1) {
							LOGGER.warn("IOException {}", e1.getMessage());
						}
					}
				}
			}
		}

		Set<String> andResultSet = new HashSet<String>();

		if (!andTerms.isEmpty())
			andResultSet = andTerms.get(0);

		for (int i = 0; i < andTerms.size(); i++) {
			andResultSet = Sets.intersection(andResultSet, andTerms.get(i));
		}

		List<Map<String, Object>> resultRows = new ArrayList<Map<String, Object>>();

		Iterator<String> iterator = andResultSet.iterator();

		while (iterator.hasNext()) {
			Map<String, Object> row = get(keySpace, authorizableColumnFamily,
					iterator.next());
			resultRows.add(row);
		}

		tResultRows = resultRows;
		final Iterator<String> fIterator = andResultSet.iterator();

		if (tResultRows.isEmpty()) {
			return new DisposableIterator<Map<String, Object>>() {

				private Disposer disposer;

				public boolean hasNext() {
					return false;
				}

				public Map<String, Object> next() {
					return null;
				}

				public void remove() {
				}

				public void close() {
					if (disposer != null) {
						disposer.unregisterDisposable(this);
					}
				}

				public void setDisposer(Disposer disposer) {
					this.disposer = disposer;
				}
			};
		}
		return registerDisposable(new PreemptiveIterator<Map<String, Object>>() {

			private Map<String, Object> nextValue = Maps.newHashMap();
			private boolean open = true;

			protected Map<String, Object> internalNext() {
				return nextValue;
			}

			protected boolean internalHasNext() {
				if (fIterator.hasNext()) {
					try {
						String id = fIterator.next();
						nextValue = get(fKeyspace, fAuthorizableColumnFamily,
								id);
						LOGGER.debug("Got Row ID {} {} ", id, nextValue);
						return true;
					} catch (StorageClientException e) {

					}
				}
				close();
				nextValue = null;
				LOGGER.debug("End of Set ");
				return false;
			}

			@Override
			public void close() {
				if (open) {
					open = false;
				}

			}
		});
	}

	public void close() {
		// TODO Auto-generated method stub

	}

	public DisposableIterator<Map<String, Object>> listChildren(
			String keySpace, String columnFamily, String key,
			DirectCacheAccess cachingManager) throws StorageClientException {
		return find(keySpace, columnFamily, ImmutableMap.<String, Object>of(
				Content.PARENT_HASH_FIELD, key), cachingManager);
	}

	public boolean hasBody(Map<String, Object> content, String streamId) {
		return false;
	}

	public DisposableIterator<SparseRow> listAll(String keySpace,
			String columnFamily) throws StorageClientException {
		final Iterator<Entry<String, Object>> i = storage.entrySet().iterator(); 
		return new DisposableIterator<SparseRow>() {

			public boolean hasNext() {
				return i.hasNext();
			}

			public SparseRow next() {
				Entry<String, Object> entry = i.next();
				@SuppressWarnings("unchecked")
				Map<String, Object> content = (Map<String, Object>) entry.getValue();
				return new SparseMapRow((String)content.get(Content.UUID_FIELD), content);
			}

			public void remove() {
				i.remove();
			}

			public void close() { }
			public void setDisposer(Disposer disposer) { }
			
		};
	}

	public long allCount(String keySpace, String columnFamily)
			throws StorageClientException {
		return storage.entrySet().size();
	}

	public void setStorageClientListener(
			StorageClientListener storageClientListener) {
		this.storageClientListener = storageClientListener;
	}

	@SuppressWarnings("unchecked")
	private Map<String, Object> getMapFromStorage(String storageKey) {
		return (Map<String, Object>) storage.get(storageKey);
	}

	private String createStorageKey(String keySpace, String columnFamily,
			String key) {
		return String.format("%s:%s:%s", keySpace, columnFamily, key);
	}

	private <T extends Disposable> T registerDisposable(T disposable) {
		toDispose.add(disposable);
		return disposable;
	}

	private void addIndex(String keySpace, String columnFamily, String key,
			String propName, byte[] b) throws StorageClientException {
		String indexKey = String.format("%s:%s:%s", propName, columnFamily, StorageClientUtils.insecureHash(b));
		Map<String, Object> values = new HashMap<String, Object>();
		values.put(key, (Object) "Whatever value of index");
		insert(keySpace, INDEX_COLUMN_FAMILY, indexKey, values, true);
	}
	
	private void removeIndex(String keySpace, String columnFamily, String key,
			String propName, byte[] b) throws StorageClientException {
		String indexKey = String.format("%s:%s:%s", propName, columnFamily, StorageClientUtils.insecureHash(b));
		Map<String, Object> index = get(keySpace, INDEX_COLUMN_FAMILY, indexKey);
		if (index != null && index.containsKey(key)) {
			index.put(key, new RemoveProperty());
			insert(keySpace, INDEX_COLUMN_FAMILY, indexKey, index, false);
		}
	}

	private boolean shouldIndex(String keySpace, String columnFamily,
			String columnName) throws StorageClientException {
		if (INTERNAL_INDEX_COLUMNS.contains(columnName)) {
			return true;
		} else if (indexColumns.contains(columnFamily + ":" + columnName)) {
			return true;
		} else {
			return false;
		}
	}

}
