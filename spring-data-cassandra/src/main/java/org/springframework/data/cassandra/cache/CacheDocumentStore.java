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

import org.jspecify.annotations.Nullable;

/**
 * Contract for persisting and loading {@link CacheDocument cache documents} from a backing store.
 *
 * @author Anıl Şenocak
 * @since 5.2
 */
public interface CacheDocumentStore {

	/**
	 * Load a single {@link CacheDocument} for the given {@code cacheName} and {@code cacheKey}.
	 *
	 * @param cacheName the logical cache name, must not be {@literal null}.
	 * @param cacheKey the serialized cache key, must not be {@literal null}.
	 * @return the matching {@link CacheDocument}, or {@literal null} if no entry exists.
	 */
	@Nullable
	CacheDocument get(final String cacheName, final String cacheKey);

	/**
	 * Persist the given {@link CacheDocument}, overwriting any existing entry with the same key.
	 *
	 * @param document the document to persist, must not be {@literal null}.
	 */
	void put(final CacheDocument document);

	/**
	 * Delete the entry identified by the given {@code cacheName} and {@code cacheKey}.
	 *
	 * @param cacheName the logical cache name, must not be {@literal null}.
	 * @param cacheKey the serialized cache key, must not be {@literal null}.
	 * @return the removed {@link CacheDocument}, or {@literal null} if no entry existed.
	 */
	@Nullable
	CacheDocument delete(final String cacheName, final String cacheKey);

	/**
	 * Load all {@link CacheDocument documents} for the given {@code cacheName}.
	 *
	 * @param cacheName the logical cache name, must not be {@literal null}.
	 * @return a {@link List} of all matching documents, never {@literal null}.
	 */
	List<CacheDocument> findAll(final String cacheName);

	/**
	 * Delete all entries for the given {@code cacheName} that expired before {@code nowMillis}.
	 *
	 * @param cacheName the logical cache name, must not be {@literal null}.
	 * @param nowMillis the reference time used to determine expiration, must not be {@literal null}.
	 * @return the number of purged entries.
	 */
	default int purgeExpired(final String cacheName, final long nowMillis) {
		return 0;
	}
}
