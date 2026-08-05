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
package org.springframework.data.cassandra.cache.event;

import java.time.Instant;
import java.util.Objects;

/**
 * Published when a value is removed from the cache, either explicitly, through a {@code clear} or by expiry eviction.
 *
 * @author Anıl Şenocak
 * @since 5.2
 * @param <K> the cache key type.
 * @param <V> the cache value type.
 */
public final class CacheEvictedEvent<K, V> implements CacheEvent<K, V> {
	private final String cacheName;
	private final K key;
	private final V value;
	private final Instant occurredAt;

	public CacheEvictedEvent(final String cacheName, final K key, final V value) {
		this(cacheName, key, value, Instant.now());
	}

	public CacheEvictedEvent(final String cacheName, final K key, final V value, final Instant occurredAt) {
		this.cacheName = Objects.requireNonNull(cacheName, "CacheName must not be null");
		this.key = Objects.requireNonNull(key, "Key must not be null");
		this.value = Objects.requireNonNull(value, "Value must not be null");
		this.occurredAt = Objects.requireNonNull(occurredAt, "OccurredAt must not be null");
	}

	@Override
	public String getCacheName() {
		return cacheName;
	}

	@Override
	public K getKey() {
		return key;
	}

	@Override
	public V getValue() {
		return value;
	}

	@Override
	public Instant getOccurredAt() {
		return occurredAt;
	}

	@Override
	public boolean equals(final Object other) {
		if (this == other) {
			return true;
		}
		if (!(other instanceof CacheEvictedEvent<?, ?> that)) {
			return false;
		}
		return this.cacheName.equals(that.cacheName) && this.key.equals(that.key) && this.value.equals(that.value)
				&& this.occurredAt.equals(that.occurredAt);
	}

	@Override
	public int hashCode() {
		return Objects.hash(cacheName, key, value, occurredAt);
	}

	@Override
	public String toString() {
		return "CacheEvictedEvent[cacheName=%s, key=%s, value=%s, occurredAt=%s]".formatted(cacheName, key, value,
				occurredAt);
	}
}
