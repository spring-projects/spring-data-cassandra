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
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.jspecify.annotations.Nullable;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.data.cassandra.core.CassandraOperations;

/**
 * {@link CacheManager} that stores all managed cache entries in Cassandra. Every named cache shares the same
 * Cassandra table, using the cache name as the partition key, so that all application instances observe the same
 * cache state.
 * <p>
 * Expired entries can be removed on a background thread by supplying a positive {@code clearInterval}. The returned
 * manager must be {@link #close() closed} (for example as a {@code destroy-method}) to stop that thread.
 *
 * @author Anıl Şenocak
 * @since 5.2
 */
public final class CassandraCacheManager implements CacheManager, AutoCloseable {
	private final Function<String, CassandraCache<String, SpringCacheEntry>> cacheFactory;
	private final ObjectMapper objectMapper;
	private final Function<Object, String> keySerializer;
	private final @Nullable ApplicationEventPublisher applicationEventPublisher;
	private final @Nullable Duration entryTtl;
	private final ConcurrentHashMap<String, Cache> caches = new ConcurrentHashMap<>();
	private final @Nullable ScheduledExecutorService clearExecutor;

	public CassandraCacheManager(final Function<String, CassandraCache<String, SpringCacheEntry>> cacheFactory,
								 final ObjectMapper objectMapper) {
		this(cacheFactory, objectMapper, Object::toString, null, null, null);
	}

	public CassandraCacheManager(final Function<String, CassandraCache<String, SpringCacheEntry>> cacheFactory,
								 final ObjectMapper objectMapper, final Function<Object, String> keySerializer,
								 final @Nullable Duration clearInterval, final @Nullable ApplicationEventPublisher applicationEventPublisher,
								 final @Nullable Duration entryTtl) {
		this.cacheFactory = Objects.requireNonNull(cacheFactory, "CacheFactory must not be null");
		this.objectMapper = Objects.requireNonNull(objectMapper, "ObjectMapper must not be null");
		this.keySerializer = Objects.requireNonNull(keySerializer, "KeySerializer must not be null");
		this.applicationEventPublisher = applicationEventPublisher;
		this.entryTtl = entryTtl;
		this.clearExecutor = schedulePeriodicClear(clearInterval);
	}

	public CassandraCacheManager(final CassandraOperations cassandraOperations) {
		this(cassandraOperations, CassandraCacheImpl.DEFAULT_TABLE_NAME);
	}

	public CassandraCacheManager(final CassandraOperations cassandraOperations, final String tableName) {
		this(cassandraOperations, tableName, CassandraCacheImpl.defaultObjectMapper(), Object::toString, null, null, null);
	}

	public CassandraCacheManager(final CassandraOperations cassandraOperations, final String tableName, final ObjectMapper objectMapper,
								 final Function<Object, String> keySerializer, final @Nullable Duration clearInterval,
								 final @Nullable ApplicationEventPublisher applicationEventPublisher, final @Nullable Duration entryTtl) {
		this(cacheName -> new CassandraCacheImpl<>(cacheName, String.class, SpringCacheEntry.class,
				new CassandraCacheDocumentStore(cassandraOperations, tableName), objectMapper,
				applicationEventPublisher, entryTtl),
				objectMapper, keySerializer, clearInterval, applicationEventPublisher, entryTtl);
	}

	@Override
	public Cache getCache(final String name) {
		return caches.computeIfAbsent(name,
				cacheName -> new CassandraBackedSpringCache(cacheName, cacheFactory.apply(cacheName), objectMapper,
						keySerializer, applicationEventPublisher));
	}

	@Override
	public Collection<String> getCacheNames() {
		return List.copyOf(caches.keySet());
	}

	/**
	 * Clear all managed caches.
	 */
	public void clearAll() {
		caches.values().forEach(Cache::clear);
	}

	/**
	 * Remove all expired entries from all managed caches.
	 *
	 * @return the number of removed entries.
	 */
	public int evictExpired() {
		return caches.values().stream()
				.mapToInt(cache -> cache instanceof CassandraBackedSpringCache cassandraCache
						? cassandraCache.evictExpired()
						: 0)
				.sum();
	}

	/**
	 * @return the number of currently managed caches.
	 */
	public int getCacheCount() {
		return caches.size();
	}

	/**
	 * @return whether a periodic cleanup task is scheduled.
	 */
	public boolean isPeriodicClearEnabled() {
		return clearExecutor != null;
	}

	@Override
	public void close() {
		if (clearExecutor != null) {
			clearExecutor.shutdownNow();
		}
	}

	private @Nullable ScheduledExecutorService schedulePeriodicClear(final @Nullable Duration interval) {
		if (interval == null || interval.isZero()) {
			return null;
		}
		if (interval.isNegative()) {
			throw new IllegalArgumentException("Clear interval must not be negative");
		}
		if (entryTtl != null && entryTtl.isNegative()) {
			throw new IllegalArgumentException("Entry TTL must not be negative");
		}
		final long delayMillis = Math.max(interval.toMillis(), 1L);
		final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(runnable -> {
			final Thread thread = new Thread(runnable, "cassandra-cache-expirer");
			thread.setDaemon(true);
			return thread;
		});
		executor.scheduleWithFixedDelay(this::evictExpired, delayMillis, delayMillis, TimeUnit.MILLISECONDS);
		return executor;
	}
}
