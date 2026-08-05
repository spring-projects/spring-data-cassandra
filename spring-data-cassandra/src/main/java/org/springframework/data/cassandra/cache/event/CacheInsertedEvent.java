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

import org.jspecify.annotations.Nullable;

/**
 * Published when a value is written to the cache. Carries the previous value, if one existed.
 *
 * @author Anıl Şenocak
 * @since 5.2
 * @param <K> the cache key type.
 * @param <V> the cache value type.
 */
public final class CacheInsertedEvent<K, V> implements CacheEvent<K, V> {
	private final String cacheName;
	private final K key;
	private final V value;
	private final @Nullable V previousValue;
	private final Instant occurredAt;

	public CacheInsertedEvent(final String cacheName, final K key, final V value) {
		this(cacheName, key, value, null, Instant.now());
	}

	public CacheInsertedEvent(final String cacheName, final K key, final V value, final @Nullable V previousValue) {
		this(cacheName, key, value, previousValue, Instant.now());
	}

	public CacheInsertedEvent(final String cacheName, final K key, final V value, final @Nullable V previousValue, final Instant occurredAt) {
		this.cacheName = Objects.requireNonNull(cacheName, "CacheName must not be null");
		this.key = Objects.requireNonNull(key, "Key must not be null");
		this.value = Objects.requireNonNull(value, "Value must not be null");
		this.previousValue = previousValue;
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

	public @Nullable V getPreviousValue() {
		return previousValue;
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
		if (!(other instanceof CacheInsertedEvent<?, ?> that)) {
			return false;
		}
		return this.cacheName.equals(that.cacheName) && this.key.equals(that.key) && this.value.equals(that.value)
				&& Objects.equals(this.previousValue, that.previousValue) && this.occurredAt.equals(that.occurredAt);
	}

	@Override
	public int hashCode() {
		return Objects.hash(cacheName, key, value, previousValue, occurredAt);
	}

	@Override
	public String toString() {
		return "CacheInsertedEvent[cacheName=%s, key=%s, value=%s, previousValue=%s, occurredAt=%s]".formatted(cacheName,
				key, value, previousValue, occurredAt);
	}
}
