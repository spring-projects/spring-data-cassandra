/*
 * Copyright 2026-present the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.cache;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.jspecify.annotations.Nullable;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.data.cassandra.cache.event.CacheEvictedEvent;
import org.springframework.data.cassandra.cache.event.CacheInsertedEvent;
import org.springframework.data.cassandra.core.CassandraOperations;

/**
 * {@link CassandraCache} implementation that persists every entry in Cassandra. Each cache instance is confined to a
 * single logical cache name identified by the {@code cacheName} partition key.
 * <p>
 * Entries expire according to the configured {@code entryTtl}. The read path treats any entry whose {@code expires_at}
 * is in the past as absent without deleting it immediately; expired rows are removed by {@link #evictExpired()}.
 *
 * @author Anıl Şenocak
 * @since 5.2
 * @param <K> the cache key type.
 * @param <V> the cache value type.
 */
public final class CassandraCacheImpl<K, V> implements CassandraCache<K, V> {
	public static final String DEFAULT_TABLE_NAME = "spring_cache_entries";

	private final String cacheName;
	private final Class<K> keyType;
	private final Class<V> valueType;
	private final CacheDocumentStore cacheStore;
	private final ObjectMapper objectMapper;
	private final @Nullable ApplicationEventPublisher applicationEventPublisher;
	private final @Nullable Duration entryTtl;
	private final ReentrantLock ioLock = new ReentrantLock();

	public CassandraCacheImpl(final String cacheName, final Class<K> keyType, final Class<V> valueType,
							 final CassandraOperations cassandraOperations) {
		this(cacheName, keyType, valueType, new CassandraCacheDocumentStore(cassandraOperations), defaultObjectMapper(),
				null, null);
	}

	public CassandraCacheImpl(final String cacheName, final Class<K> keyType, final Class<V> valueType,
							  final CassandraOperations cassandraOperations, final String tableName) {
		this(cacheName, keyType, valueType, new CassandraCacheDocumentStore(cassandraOperations, tableName),
				defaultObjectMapper(), null, null);
	}

	public CassandraCacheImpl(final String cacheName, final Class<K> keyType, final Class<V> valueType,
							  final CassandraOperations cassandraOperations, final String tableName, final ObjectMapper objectMapper,
							  final @Nullable ApplicationEventPublisher applicationEventPublisher, final @Nullable Duration entryTtl) {
		this(cacheName, keyType, valueType, new CassandraCacheDocumentStore(cassandraOperations, tableName), objectMapper,
				applicationEventPublisher, entryTtl);
	}

	public CassandraCacheImpl(final String cacheName, final Class<K> keyType, final Class<V> valueType,
							  final CacheDocumentStore cacheStore) {
		this(cacheName, keyType, valueType, cacheStore, defaultObjectMapper(), null, null);
	}

	public CassandraCacheImpl(final String cacheName, final Class<K> keyType, final Class<V> valueType,
							  final CacheDocumentStore cacheStore, final ObjectMapper objectMapper) {
		this(cacheName, keyType, valueType, cacheStore, objectMapper, null, null);
	}

	public CassandraCacheImpl(final String cacheName, final Class<K> keyType, final Class<V> valueType,
	                          final CacheDocumentStore cacheStore, final ObjectMapper objectMapper,
	                          final @Nullable ApplicationEventPublisher applicationEventPublisher, final @Nullable Duration entryTtl) {
		Objects.requireNonNull(keyType, "KeyType must not be null");
		Objects.requireNonNull(valueType, "ValueType must not be null");
		Objects.requireNonNull(cacheStore, "CacheStore must not be null");
		Objects.requireNonNull(objectMapper, "ObjectMapper must not be null");
		if (entryTtl != null && entryTtl.isNegative()) {
			throw new IllegalArgumentException("Entry TTL must not be negative");
		}
		this.cacheName = validateCacheName(cacheName);
		this.keyType = keyType;
		this.valueType = valueType;
		this.cacheStore = cacheStore;
		this.objectMapper = objectMapper;
		this.applicationEventPublisher = applicationEventPublisher;
		this.entryTtl = entryTtl;
	}

	@Override
	public @Nullable V get(final K key) {
		ioLock.lock();
		try {
			final CacheDocument document = cacheStore.get(cacheName, toStoredKey(key));
			if (document == null || isExpired(document, nowMillis())) {
				return null;
			}
			return fromStoredValue(document.valueJson());
		} finally {
			ioLock.unlock();
		}
	}

	@Override
	public void put(K key, V value) {
		CacheInsertedEvent<K, V> event;
		ioLock.lock();
		try {
			final long nowMillis = nowMillis();
			final String storedKey = toStoredKey(key);
			final CacheDocument previousDocument = cacheStore.get(cacheName, storedKey);
			final V previousValue = previousDocument == null || isExpired(previousDocument, nowMillis)
					? null
					: fromStoredValue(previousDocument.valueJson());
			cacheStore.put(new CacheDocument(cacheName, storedKey, storedKey, toStoredValue(value),
					resolveExpiresAt(nowMillis)));
			event = new CacheInsertedEvent<>(cacheName, key, value, previousValue);
		} finally {
			ioLock.unlock();
		}
		fireEvent(event);
	}

	@Override
	public @Nullable V evict(final K key) {
		CacheEvictedEvent<K, V> event;
		ioLock.lock();
		try {
			final CacheDocument removedDocument = cacheStore.delete(cacheName, toStoredKey(key));
			if (removedDocument == null) {
				return null;
			}
			event = new CacheEvictedEvent<>(cacheName, key, fromStoredValue(removedDocument.valueJson()));
		} finally {
			ioLock.unlock();
		}
		fireEvent(event);
		return event.getValue();
	}

	@Override
	public List<Map.Entry<K, V>> evictExpired() {
		final List<CacheEvictedEvent<K, V>> events = new ArrayList<>();
		ioLock.lock();
		try {
			long nowMillis = nowMillis();
			for (CacheDocument document : cacheStore.findAll(cacheName)) {
				if (!isExpired(document, nowMillis)) {
					continue;
				}
				final CacheDocument removedDocument = cacheStore.delete(cacheName, document.cacheKey());
				if (removedDocument != null) {
					events.add(new CacheEvictedEvent<>(cacheName, fromStoredKey(removedDocument.keyJson()),
							fromStoredValue(removedDocument.valueJson())));
				}
			}
		} finally {
			ioLock.unlock();
		}
		events.forEach(this::fireEvent);
		return events.stream().map(event -> Map.entry(event.getKey(), event.getValue())).toList();
	}

	@Override
	public void clear() {
		final List<CacheEvictedEvent<K, V>> events = new ArrayList<>();
		ioLock.lock();
		try {
			for (CacheDocument document : cacheStore.findAll(cacheName)) {
				final CacheDocument removedDocument = cacheStore.delete(cacheName, document.cacheKey());
				if (removedDocument != null) {
					events.add(new CacheEvictedEvent<>(cacheName, fromStoredKey(removedDocument.keyJson()),
							fromStoredValue(removedDocument.valueJson())));
				}
			}
		} finally {
			ioLock.unlock();
		}
		events.forEach(this::fireEvent);
	}

	@Override
	public boolean containsKey(K key) {
		ioLock.lock();
		try {
			final CacheDocument document = cacheStore.get(cacheName, toStoredKey(key));
			return document != null && !isExpired(document, nowMillis());
		} finally {
			ioLock.unlock();
		}
	}

	@Override
	public int size() {
		ioLock.lock();
		try {
			final long nowMillis = nowMillis();
			return (int) cacheStore.findAll(cacheName).stream()
					.filter(document -> !isExpired(document, nowMillis))
					.count();
		} finally {
			ioLock.unlock();
		}
	}

	@Override
	public Set<K> keys() {
		ioLock.lock();
		try {
			final long nowMillis = nowMillis();
			final LinkedHashSet<K> keys = new LinkedHashSet<>();
			cacheStore.findAll(cacheName).stream()
					.filter(document -> !isExpired(document, nowMillis))
					.sorted(Comparator.comparing(CacheDocument::cacheKey))
					.map(document -> fromStoredKey(document.keyJson()))
					.forEach(keys::add);
			return keys;
		} finally {
			ioLock.unlock();
		}
	}

	/**
	 * Create an {@link ObjectMapper} pre-configured for typical cache value serialization. All modules on the classpath
	 * are registered (for example Java time support) and unknown properties are ignored on deserialization.
	 *
	 * @return a configured {@link ObjectMapper}, never {@literal null}.
	 */
	public static ObjectMapper defaultObjectMapper() {
		return new ObjectMapper()
				.findAndRegisterModules()
				.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
				.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
	}

	private @Nullable Long resolveExpiresAt(final long nowMillis) {
		if (entryTtl == null || entryTtl.isZero()) {
			return null;
		}
		return nowMillis + Math.max(entryTtl.toMillis(), 1L);
	}

	private boolean isExpired(final CacheDocument document, final long nowMillis) {
		return document.expiresAt() != null && document.expiresAt() <= nowMillis;
	}

	private String toStoredKey(final K key) {
		return writeValue(key);
	}

	private K fromStoredKey(final String keyJson) {
		return readValue(keyJson, keyType);
	}

	private String toStoredValue(final V value) {
		return writeValue(value);
	}

	private V fromStoredValue(final String valueJson) {
		return readValue(valueJson, valueType);
	}

	private String writeValue(final Object value) {
		try {
			return objectMapper.writeValueAsString(value);
		} catch (JsonProcessingException exception) {
			throw new IllegalStateException("Failed to serialize cache value", exception);
		}
	}

	private <T> T readValue(final String json, final Class<T> type) {
		try {
			return objectMapper.readValue(json, type);
		} catch (JsonProcessingException exception) {
			throw new IllegalStateException("Failed to deserialize cache value", exception);
		}
	}

	private long nowMillis() {
		return System.currentTimeMillis();
	}

	private void fireEvent(final Object event) {
		if (applicationEventPublisher != null) {
			applicationEventPublisher.publishEvent(event);
		}
	}

	private static String validateCacheName(final String cacheName) {
		Objects.requireNonNull(cacheName, "CacheName must not be null");
		if (cacheName.isBlank() || !cacheName
				.matches("[A-Za-z0-9_\\-.]+")) {
			throw new IllegalArgumentException(
					"Cache name '%s' may only contain letters, digits, dots, dashes, and underscores".formatted(cacheName));
		}
		return cacheName;
	}
}
