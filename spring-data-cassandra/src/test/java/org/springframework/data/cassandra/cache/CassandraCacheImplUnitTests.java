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
import java.time.LocalDate;
import java.time.Month;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import org.springframework.data.cassandra.cache.event.CacheEvictedEvent;
import org.springframework.data.cassandra.cache.event.CacheInsertedEvent;

/**
 * Unit tests for {@link CassandraCacheImpl}.
 *
 * @author Anıl Şenocak
 */
class CassandraCacheImplUnitTests {

	@Test
	void persistsAndReloadsValues() {

		InMemoryCacheDocumentStore store = new InMemoryCacheDocumentStore();
		CassandraCacheImpl<String, CachedUser> cache = newUserCache(store, null, null);

		cache.put("1", new CachedUser("1", "ada"));

		CassandraCacheImpl<String, CachedUser> reloaded = newUserCache(store, null, null);

		assertThat(reloaded.get("1")).isEqualTo(new CachedUser("1", "ada"));
		assertThat(reloaded.containsKey("1")).isTrue();
		assertThat(reloaded.keys()).containsExactly("1");
	}

	@Test
	void firesInsertedAndEvictedEvents() {

		RecordingApplicationEventPublisher publisher = new RecordingApplicationEventPublisher();
		CassandraCacheImpl<String, CachedUser> cache = newUserCache(new InMemoryCacheDocumentStore(), publisher, null);

		cache.put("1", new CachedUser("1", "ada"));
		cache.put("1", new CachedUser("1", "grace"));
		cache.evict("1");

		assertThat(publisher.events.get(0)).isInstanceOf(CacheInsertedEvent.class);
		CacheInsertedEvent<String, CachedUser> inserted = (CacheInsertedEvent<String, CachedUser>) publisher.events.get(0);
		assertThat(inserted.getCacheName()).isEqualTo("users");
		assertThat(inserted.getKey()).isEqualTo("1");
		assertThat(inserted.getValue()).isEqualTo(new CachedUser("1", "ada"));
		assertThat(inserted.getPreviousValue()).isNull();

		assertThat(publisher.events.get(1)).isInstanceOf(CacheInsertedEvent.class);
		CacheInsertedEvent<String, CachedUser> updated = (CacheInsertedEvent<String, CachedUser>) publisher.events.get(1);
		assertThat(updated.getValue()).isEqualTo(new CachedUser("1", "grace"));
		assertThat(updated.getPreviousValue()).isEqualTo(new CachedUser("1", "ada"));

		assertThat(publisher.events.get(2)).isInstanceOf(CacheEvictedEvent.class);
		CacheEvictedEvent<String, CachedUser> evicted = (CacheEvictedEvent<String, CachedUser>) publisher.events.get(2);
		assertThat(evicted.getKey()).isEqualTo("1");
		assertThat(evicted.getValue()).isEqualTo(new CachedUser("1", "grace"));
	}

	@Test
	void clearsEveryPersistedEntryAndPublishesEvents() {

		RecordingApplicationEventPublisher publisher = new RecordingApplicationEventPublisher();
		InMemoryCacheDocumentStore store = new InMemoryCacheDocumentStore();
		CassandraCacheImpl<String, CachedUser> cache = newUserCache(store, publisher, null);

		cache.put("1", new CachedUser("1", "ada"));
		cache.put("2", new CachedUser("2", "grace"));
		publisher.events.clear();

		cache.clear();

		assertThat(publisher.events).hasSize(2).allMatch(CacheEvictedEvent.class::isInstance);
		assertThat(newUserCache(store, null, null).size()).isZero();
	}

	@Test
	void readsCurrentValuesFromStoreForEveryGet() {

		InMemoryCacheDocumentStore store = new InMemoryCacheDocumentStore();
		CassandraCacheImpl<String, CachedUser> first = newUserCache(store, null, null);
		CassandraCacheImpl<String, CachedUser> second = newUserCache(store, null, null);

		first.put("1", new CachedUser("1", "ada"));
		second.put("1", new CachedUser("1", "grace"));

		assertThat(first.get("1")).isEqualTo(new CachedUser("1", "grace"));
	}

	@Test
	void validatesCacheNames() {

		assertThatIllegalArgumentException().isThrownBy(() -> new CassandraCacheImpl<>(" ", String.class, CachedUser.class,
				new InMemoryCacheDocumentStore()));
		assertThatIllegalArgumentException().isThrownBy(
				() -> new CassandraCacheImpl<>("users/active", String.class, CachedUser.class,
						new InMemoryCacheDocumentStore()));

		CassandraCacheImpl<String, CachedUser> cache = new CassandraCacheImpl<>("users-v1.active_cache", String.class,
				CachedUser.class, new InMemoryCacheDocumentStore());

		cache.put("1", new CachedUser("1", "ada"));
		assertThat(cache.get("1")).isEqualTo(new CachedUser("1", "ada"));
	}

	@Test
	void handlesMissingKeysAndEmptyStores() {

		CassandraCacheImpl<String, CachedUser> cache = newUserCache(new InMemoryCacheDocumentStore(), null, null);

		assertThat(cache.size()).isZero();
		assertThat(cache.keys()).isEmpty();

		cache.put("1", new CachedUser("1", "ada"));

		assertThat(cache.evict("missing")).isNull();
		assertThat(cache.size()).isEqualTo(1);
	}

	@Test
	void expiresValuesAfterEntryTtl() throws InterruptedException {

		CassandraCacheImpl<String, CachedUser> cache = newUserCache(new InMemoryCacheDocumentStore(), null,
				Duration.ofMillis(25));

		cache.put("1", new CachedUser("1", "ada"));
		Thread.sleep(50);

		assertThat(cache.get("1")).isNull();
		assertThat(cache.containsKey("1")).isFalse();
		assertThat(cache.size()).isZero();
		assertThat(cache.evictExpired()).containsExactly(Map.entry("1", new CachedUser("1", "ada")));
		assertThat(cache.evictExpired()).isEmpty();
	}

	@Test
	void zeroEntryTtlDoesNotExpireValues() throws InterruptedException {

		CassandraCacheImpl<String, CachedUser> cache = newUserCache(new InMemoryCacheDocumentStore(), null,
				Duration.ZERO);

		cache.put("1", new CachedUser("1", "ada"));
		Thread.sleep(25);

		assertThat(cache.get("1")).isEqualTo(new CachedUser("1", "ada"));
		assertThat(cache.evictExpired()).isEmpty();
	}

	@Test
	void persistsJavaTimeValues() {

		InMemoryCacheDocumentStore store = new InMemoryCacheDocumentStore();
		CassandraCacheImpl<String, CachedEvent> cache = new CassandraCacheImpl<>("events", String.class, CachedEvent.class,
				store);
		CachedEvent event = new CachedEvent("1", LocalDate.of(2026, Month.JUNE, 10));

		cache.put("1", event);

		assertThat(new CassandraCacheImpl<>("events", String.class, CachedEvent.class, store).get("1")).isEqualTo(event);
	}

	private CassandraCacheImpl<String, CachedUser> newUserCache(CacheDocumentStore store,
			RecordingApplicationEventPublisher publisher, Duration entryTtl) {

		return new CassandraCacheImpl<>("users", String.class, CachedUser.class, store,
				CassandraCacheImpl.defaultObjectMapper(), publisher, entryTtl);
	}

	record CachedUser(String id, String username) {
	}

	record CachedEvent(String id, LocalDate happenedOn) {
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
