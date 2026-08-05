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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.function.Function;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.Cache;
import org.springframework.cache.support.SimpleValueWrapper;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.data.cassandra.cache.event.CacheEvictedEvent;
import org.springframework.data.cassandra.cache.event.CacheInsertedEvent;

/**
 * {@link Cache} adapter backed by a {@link CassandraCache}. Arbitrary values are wrapped in a {@link SpringCacheEntry}
 * envelope together with their type so they can be restored after a reload from Cassandra.
 *
 * @author Anıl Şenocak
 * @since 5.2
 */
public final class CassandraBackedSpringCache implements Cache {
	private static final Logger log = LoggerFactory.getLogger(CassandraBackedSpringCache.class);

	private final String name;
	private final CassandraCache<String, SpringCacheEntry> cache;
	private final ObjectMapper objectMapper;
	private final Function<Object, String> keySerializer;
	private final @Nullable ApplicationEventPublisher applicationEventPublisher;

	public CassandraBackedSpringCache(final String name, final CassandraCache<String, SpringCacheEntry> cache,
									  final ObjectMapper objectMapper) {
		this(name, cache, objectMapper, Object::toString, null);
	}

	public CassandraBackedSpringCache(final String name, final CassandraCache<String, SpringCacheEntry> cache,
									  final ObjectMapper objectMapper, final Function<Object, String> keySerializer,
									  final @Nullable ApplicationEventPublisher applicationEventPublisher) {
		this.name = Objects.requireNonNull(name, "Name must not be null");
		this.cache = Objects.requireNonNull(cache, "Cache must not be null");
		this.objectMapper = Objects.requireNonNull(objectMapper, "ObjectMapper must not be null");
		this.keySerializer = Objects.requireNonNull(keySerializer, "KeySerializer must not be null");
		this.applicationEventPublisher = applicationEventPublisher;
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public Object getNativeCache() {
		return cache;
	}

	@Override
	public @Nullable ValueWrapper get(final Object key) {
		final SpringCacheEntry entry = cache.get(toCacheKey(key));
		return entry == null ? null : new SimpleValueWrapper(toValue(entry));
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T> @Nullable T get(Object key, @Nullable Class<T> type) {
		final SpringCacheEntry entry = cache.get(toCacheKey(key));
		if (entry == null) {
			return null;
		}
		final Object value = toValue(entry);
		if (type == null) {
			return (T) value;
		}
		return type.isInstance(value) ? type.cast(value) : null;
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T> T get(final Object key, final Callable<T> valueLoader) {
		final String cacheKey = toCacheKey(key);
		final SpringCacheEntry entry = cache.get(cacheKey);
		if (entry != null) {
			return (T) toValue(entry);
		}
		try {
			final T loadedValue = valueLoader.call();
			put(key, loadedValue);
			return loadedValue;
		} catch (Exception exception) {
			throw new ValueRetrievalException(key, valueLoader, exception);
		}
	}

	@Override
	public void put(final Object key, final @Nullable Object value) {
		if (value == null) {
			return;
		}
		final String cacheKey = toCacheKey(key);
		final SpringCacheEntry previousEntry = cache.get(cacheKey);
		final Object previousValue = previousEntry == null ? null : toValue(previousEntry);
		cache.put(cacheKey, new SpringCacheEntry(value.getClass().getName(), objectMapper.valueToTree(value)));
		publishEvent(new CacheInsertedEvent<>(name, cacheKey, value, previousValue));
	}

	@Override
	public void evict(final Object key) {
		final String cacheKey = toCacheKey(key);
		final SpringCacheEntry removed = cache.evict(cacheKey);
		if (removed != null) {
			publishEvent(new CacheEvictedEvent<>(name, cacheKey, toValue(removed)));
		}
	}

	@Override
	public void clear() {
		final List<CacheEvictedEvent<String, Object>> events = new ArrayList<>();
		for (String cacheKey : cache.keys()) {
			final SpringCacheEntry entry = cache.get(cacheKey);
			if (entry != null) {
				events.add(new CacheEvictedEvent<>(name, cacheKey, toValue(entry)));
			}
		}
		cache.clear();
		events.forEach(this::publishEvent);
	}

	@Override
	public boolean invalidate() {
		clear();
		return true;
	}

	/**
	 * Remove all expired entries from this cache.
	 *
	 * @return the number of removed entries.
	 */
	public int evictExpired() {
		final List<CacheEvictedEvent<String, Object>> events = cache.evictExpired().stream()
				.map(entry -> new CacheEvictedEvent<>(name, entry.getKey(), toValue(entry.getValue())))
				.toList();
		if (!events.isEmpty()) {
			log.info("Evicting {} expired entries from cache [{}]", events.size(), name);
		}
		events.forEach(this::publishEvent);
		return events.size();
	}

	private String toCacheKey(final Object key) {
		return keySerializer.apply(key);
	}

	private Object toValue(final SpringCacheEntry entry) {
		try {
			final Class<?> valueClass = Class.forName(entry.getType());
			return objectMapper.treeToValue(entry.getValue(), valueClass);
		} catch (ClassNotFoundException | JsonProcessingException exception) {
			throw new IllegalStateException("Failed to deserialize cache entry for cache [%s]".formatted(name), exception);
		}
	}

	private void publishEvent(final Object event) {
		if (applicationEventPublisher != null) {
			applicationEventPublisher.publishEvent(event);
		}
	}
}
