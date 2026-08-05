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

import static org.assertj.core.api.Assertions.*;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import org.junit.jupiter.api.Test;

import org.springframework.cache.Cache;
import org.springframework.data.cassandra.cache.event.CacheEvictedEvent;
import org.springframework.data.cassandra.cache.event.CacheInsertedEvent;

/**
 * Unit tests for {@link CassandraBackedSpringCache}.
 *
 * @author Anıl Şenocak
 */
class CassandraBackedSpringCacheUnitTests {

	@Test
	void reloadsTypedValuesFromManagedCaches() {

		Map<String, InMemoryCassandraCache> backingCaches = new LinkedHashMap<>();
		Cache cache = managerUsing(backingCaches).getCache("users");

		cache.put("42", new CachedUser("42", "ada"));

		Object value = managerUsing(backingCaches).getCache("users").get("42").get();

		assertThat(value).isInstanceOf(CachedUser.class).isEqualTo(new CachedUser("42", "ada"));
	}

	@Test
	void usesLoaderOnlyWhenKeyIsMissing() {

		Cache cache = managerUsing(new LinkedHashMap<>()).getCache("users");
		CachedUser loaded = cache.get("42", () -> new CachedUser("42", "ada"));

		assertThat(loaded).isEqualTo(new CachedUser("42", "ada"));
		assertThat(cache.get("42", CachedUser.class)).isEqualTo(loaded);
		CachedUser cached = cache.get("42", () -> {
			throw new IllegalStateException("not called");
		});
		assertThat(cached).isEqualTo(loaded);
	}

	@Test
	void returnsWrappedTypedAndUntypedValues() {

		InMemoryCassandraCache backingCache = new InMemoryCassandraCache();
		CassandraBackedSpringCache cache = new CassandraBackedSpringCache("users", backingCache,
				CassandraCacheImpl.defaultObjectMapper());
		CachedUser user = new CachedUser("42", "ada");

		cache.put("42", user);

		assertThat(cache.getName()).isEqualTo("users");
		assertThat(cache.getNativeCache()).isSameAs(backingCache);
		assertThat(cache.get("42").get()).isEqualTo(user);
		assertThat(cache.get("42", CachedUser.class)).isEqualTo(user);
		assertThat(cache.get("42", (Class<Object>) null)).isEqualTo(user);
		assertThat(cache.get("42", String.class)).isNull();
		assertThat(cache.get("missing")).isNull();
	}

	@Test
	void doesNotCacheNullValues() {

		InMemoryCassandraCache backingCache = new InMemoryCassandraCache();
		CassandraBackedSpringCache cache = new CassandraBackedSpringCache("users", backingCache,
				CassandraCacheImpl.defaultObjectMapper());

		cache.put("42", null);

		assertThat(backingCache.containsKey("42")).isFalse();
		assertThat(cache.get("42")).isNull();
	}

	@Test
	void wrapsLoaderExceptions() {

		CassandraBackedSpringCache cache = new CassandraBackedSpringCache("users", new InMemoryCassandraCache(),
				CassandraCacheImpl.defaultObjectMapper());
		java.util.concurrent.		Callable<CachedUser> failingLoader = () -> {
			throw new IllegalStateException("boom");
		};

		Cache.ValueRetrievalException exception = catchThrowableOfType(() -> cache.get("42", failingLoader),
				Cache.ValueRetrievalException.class);

		assertThat(exception.getCause()).isInstanceOf(IllegalStateException.class).hasMessage("boom");
	}

	@Test
	void evictsClearsAndInvalidatesEntries() {

		InMemoryCassandraCache backingCache = new InMemoryCassandraCache();
		CassandraBackedSpringCache cache = new CassandraBackedSpringCache("users", backingCache,
				CassandraCacheImpl.defaultObjectMapper());

		cache.put("42", new CachedUser("42", "ada"));
		cache.evict("42");

		assertThat(cache.get("42")).isNull();

		cache.put("43", new CachedUser("43", "grace"));
		cache.clear();

		assertThat(backingCache.size()).isZero();

		cache.put("44", new CachedUser("44", "katherine"));

		assertThat(cache.invalidate()).isTrue();
		assertThat(backingCache.size()).isZero();
	}

	@Test
	void publishesDecodedSpringCacheEvents() {

		RecordingApplicationEventPublisher publisher = new RecordingApplicationEventPublisher();
		CassandraBackedSpringCache cache = new CassandraBackedSpringCache("users", new InMemoryCassandraCache(),
				CassandraCacheImpl.defaultObjectMapper(), Object::toString, publisher);

		cache.put("42", new CachedUser("42", "ada"));
		cache.put("42", new CachedUser("42", "grace"));
		cache.evict("42");

		assertThat(publisher.events.get(0)).isInstanceOf(CacheInsertedEvent.class);
		CacheInsertedEvent<String, Object> inserted = (CacheInsertedEvent<String, Object>) publisher.events.get(0);
		assertThat(inserted.getValue()).isEqualTo(new CachedUser("42", "ada"));
		assertThat(inserted.getPreviousValue()).isNull();

		assertThat(publisher.events.get(1)).isInstanceOf(CacheInsertedEvent.class);
		CacheInsertedEvent<String, Object> updated = (CacheInsertedEvent<String, Object>) publisher.events.get(1);
		assertThat(updated.getPreviousValue()).isEqualTo(new CachedUser("42", "ada"));

		assertThat(publisher.events.get(2)).isInstanceOf(CacheEvictedEvent.class);
		CacheEvictedEvent<String, Object> evicted = (CacheEvictedEvent<String, Object>) publisher.events.get(2);
		assertThat(evicted.getValue()).isEqualTo(new CachedUser("42", "grace"));
	}

	private CassandraCacheManager managerUsing(Map<String, InMemoryCassandraCache> backingCaches) {

		return new CassandraCacheManager(
				cacheName -> backingCaches.computeIfAbsent(cacheName, ignored -> new InMemoryCassandraCache()),
				CassandraCacheImpl.defaultObjectMapper());
	}

	record CachedUser(String id, String username) {
	}

	static final class InMemoryCassandraCache implements CassandraCache<String, SpringCacheEntry> {

		private final Map<String, SpringCacheEntry> entries = new LinkedHashMap<>();

		@Override
		public SpringCacheEntry get(String key) {
			return entries.get(key);
		}

		@Override
		public void put(String key, SpringCacheEntry value) {
			entries.put(key, value);
		}

		@Override
		public SpringCacheEntry evict(String key) {
			return entries.remove(key);
		}

		@Override
		public void clear() {
			entries.clear();
		}

		@Override
		public boolean containsKey(String key) {
			return entries.containsKey(key);
		}

		@Override
		public int size() {
			return entries.size();
		}

		@Override
		public Set<String> keys() {
			return entries.keySet();
		}
	}
}
