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

/**
 * Base contract for cache lifecycle events published by the Cassandra-backed cache support.
 *
 * @author Anıl Şenocak
 * @since 5.2
 * @param <K> the cache key type.
 * @param <V> the cache value type.
 */
public sealed interface CacheEvent<K, V> permits CacheInsertedEvent, CacheEvictedEvent {

	String getCacheName();

	K getKey();

	V getValue();

	Instant getOccurredAt();
}
