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

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jspecify.annotations.Nullable;

/**
 * Typed cache contract whose state is persisted by a {@link CacheDocumentStore}.
 *
 * @author Anıl Şenocak
 * @since 5.2
 * @param <K> the cache key type.
 * @param <V> the cache value type.
 */
public interface CassandraCache<K, V> {

	/**
	 * Read the value for the given {@code key}.
	 *
	 * @param key the cache key, must not be {@literal null}.
	 * @return the cached value, or {@literal null} if absent or expired.
	 */
	@Nullable
	V get(final K key);

	/**
	 * Write the given {@code value} for the {@code key}.
	 *
	 * @param key the cache key, must not be {@literal null}.
	 * @param value the value to cache, must not be {@literal null}.
	 */
	void put(final K key, final V value);

	/**
	 * Remove the entry for the given {@code key}.
	 *
	 * @param key the cache key, must not be {@literal null}.
	 * @return the removed value, or {@literal null} if no entry existed.
	 */
	@Nullable
	V evict(final K key);

	/**
	 * Remove all entries that have expired. Returns the entries that were removed.
	 *
	 * @return a {@link List} of removed entries, never {@literal null}.
	 */
	default List<Map.Entry<K, V>> evictExpired() {
		return List.of();
	}

	/**
	 * Remove all entries.
	 */
	void clear();

	/**
	 * @param key the cache key, must not be {@literal null}.
	 * @return whether an unexpired entry exists for the given {@code key}.
	 */
	boolean containsKey(final K key);

	/**
	 * @return the number of unexpired entries.
	 */
	int size();

	/**
	 * @return a {@link Set} of all unexpired keys.
	 */
	Set<K> keys();
}
