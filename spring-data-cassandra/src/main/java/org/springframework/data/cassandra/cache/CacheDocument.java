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

import java.util.Objects;

import org.jspecify.annotations.Nullable;

/**
 * A single persisted cache entry stored as one Cassandra row.
 *
 * @author Anıl Şenocak
 * @since 5.2
 * @param cacheName the logical cache name (partition key).
 * @param cacheKey the serialized lookup key (clustering key).
 * @param keyJson the JSON representation of the original key.
 * @param valueJson the JSON representation of the cached value.
 * @param expiresAt the epoch millis at which the entry expires, or {@literal null} if it never expires.
 */
public record CacheDocument(String cacheName, String cacheKey, String keyJson, String valueJson, @Nullable Long expiresAt) {
	public CacheDocument {
		Objects.requireNonNull(cacheName, "CacheName must not be null");
		Objects.requireNonNull(cacheKey, "CacheKey must not be null");
		Objects.requireNonNull(keyJson, "KeyJson must not be null");
		Objects.requireNonNull(valueJson, "ValueJson must not be null");
	}
}
