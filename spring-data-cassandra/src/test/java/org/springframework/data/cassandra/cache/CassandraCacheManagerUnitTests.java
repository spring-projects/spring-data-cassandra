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

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import org.springframework.cache.Cache;
import org.springframework.data.cassandra.cache.event.CacheEvictedEvent;
import org.springframework.data.cassandra.cache.event.CacheInsertedEvent;

/**
 * Unit tests for {@link CassandraCacheManager}.
 *
 * @author Anıl Şenocak
 */
class CassandraCacheManagerUnitTests {

	@Test
	void reusesCachesAndReportsCacheNames() {

		CassandraCacheManager manager = managerFor(new InMemoryCacheDocumentStore(), Object::toString, null, null, null);

		Cache users = manager.getCache("users");

		assertThat(manager.getCache("users")).isSameAs(users);

		manager.getCache("products");

		assertThat(manager.getCacheCount()).isEqualTo(2);
		assertThat(manager.getCacheNames()).containsExactlyInAnyOrderElementsOf(Set.of("users", "products"));
	}

	@Test
	void clearsAllManagedCaches() {

		CassandraCacheManager manager = managerFor(new InMemoryCacheDocumentStore(), Object::toString, null, null, null);
		Cache users = manager.getCache("users");
		Cache products = manager.getCache("products");

		users.put("1", new CachedUser("1", "ada"));
		products.put("2", new CachedUser("2", "grace"));

		manager.clearAll();

		assertThat(users.get("1")).isNull();
		assertThat(products.get("2")).isNull();
	}

	@Test
	void usesCustomKeySerializerAndPersistsValues() {

		InMemoryCacheDocumentStore store = new InMemoryCacheDocumentStore();
		Function<Object, String> serializer = key -> ((LookupKey) key).id();
		CassandraCacheManager manager = managerFor(store, serializer, null, null, null);

		manager.getCache("users").put(new LookupKey("42"), new CachedUser("42", "ada"));

		CachedUser actual = managerFor(store, serializer, null, null, null).getCache("users")
				.get(new LookupKey("42"), CachedUser.class);

		assertThat(actual).isEqualTo(new CachedUser("42", "ada"));
	}

	@Test
	void publishesManagedCacheEvents() {

		RecordingApplicationEventPublisher publisher = new RecordingApplicationEventPublisher();
		CassandraCacheManager manager = managerFor(new InMemoryCacheDocumentStore(), Object::toString, null, publisher,
				null);
		Cache cache = manager.getCache("users");

		cache.put("42", new CachedUser("42", "ada"));
		cache.evict("42");

		assertThat(publisher.events.get(0)).isInstanceOf(CacheInsertedEvent.class);
		assertThat(publisher.events.get(1)).isInstanceOf(CacheEvictedEvent.class);
	}

	@Test
	void evictsOnlyExpiredEntriesAcrossCaches() throws InterruptedException {

		CassandraCacheManager manager = managerFor(new InMemoryCacheDocumentStore(), Object::toString, null, null,
				Duration.ofMillis(50));
		Cache users = manager.getCache("users");
		Cache products = manager.getCache("products");

		users.put("1", new CachedUser("1", "ada"));
		Thread.sleep(75);
		products.put("2", new CachedUser("2", "grace"));

		assertThat(manager.evictExpired()).isEqualTo(1);
		assertThat(users.get("1")).isNull();
		assertThat(products.get("2", CachedUser.class)).isEqualTo(new CachedUser("2", "grace"));
	}

	@Test
	void periodicallyEvictsExpiredEntries() throws InterruptedException {

		CassandraCacheManager manager = managerFor(new InMemoryCacheDocumentStore(), Object::toString,
				Duration.ofMillis(10), null, Duration.ofMillis(30));

		try {

			Cache users = manager.getCache("users");
			users.put("1", new CachedUser("1", "ada"));

			assertThat(manager.isPeriodicClearEnabled()).isTrue();
			assertEventually(Duration.ofSeconds(2), () -> users.get("1") == null);
		} finally {
			manager.close();
		}
	}

	@Test
	void doesNotScheduleWithoutPositiveInterval() {

		CassandraCacheManager absent = managerFor(new InMemoryCacheDocumentStore(), Object::toString, null, null, null);
		CassandraCacheManager zero = managerFor(new InMemoryCacheDocumentStore(), Object::toString, Duration.ZERO, null,
				Duration.ZERO);

		try {
			assertThat(absent.isPeriodicClearEnabled()).isFalse();
			assertThat(zero.isPeriodicClearEnabled()).isFalse();
		} finally {
			absent.close();
			zero.close();
		}
	}

	private CassandraCacheManager managerFor(CacheDocumentStore store, Function<Object, String> keySerializer,
			Duration clearInterval, RecordingApplicationEventPublisher publisher, Duration entryTtl) {

		ObjectMapper objectMapper = CassandraCacheImpl.defaultObjectMapper();

		return new CassandraCacheManager(
				cacheName -> new CassandraCacheImpl<>(cacheName, String.class, SpringCacheEntry.class, store, objectMapper,
						null, entryTtl),
				objectMapper, keySerializer, clearInterval, publisher, entryTtl);
	}

	private void assertEventually(Duration timeout, Condition condition) throws InterruptedException {

		long deadline = System.nanoTime() + timeout.toNanos();

		while (System.nanoTime() < deadline) {

			if (condition.evaluate()) {
				return;
			}

			Thread.sleep(10);
		}

		assertThat(condition.evaluate()).as("Condition was not met within %s", timeout).isTrue();
	}

	@FunctionalInterface
	interface Condition {

		boolean evaluate();
	}

	record CachedUser(String id, String username) {
	}

	record LookupKey(String id) {
	}

	static final class InMemoryCacheDocumentStore implements CacheDocumentStore {

		private final Map<String, CacheDocument> documents = new LinkedHashMap<>();

		@Override
		public CacheDocument get(String cacheName, String cacheKey) {
			return documents.get(documentKey(cacheName, cacheKey));
		}

		@Override
		public void put(CacheDocument document) {
			documents.put(documentKey(document.cacheName(), document.cacheKey()), document);
		}

		@Override
		public CacheDocument delete(String cacheName, String cacheKey) {
			return documents.remove(documentKey(cacheName, cacheKey));
		}

		@Override
		public List<CacheDocument> findAll(String cacheName) {

			return documents.values().stream().filter(document -> document.cacheName().equals(cacheName)).toList();
		}

		private String documentKey(String cacheName, String cacheKey) {
			return cacheName + "::" + cacheKey;
		}
	}
}
