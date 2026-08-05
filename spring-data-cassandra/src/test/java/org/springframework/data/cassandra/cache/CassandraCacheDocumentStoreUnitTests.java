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
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.core.cql.CqlOperations;
import org.springframework.data.cassandra.core.cql.RowMapper;

import com.datastax.oss.driver.api.core.cql.Row;

/**
 * Unit tests for {@link CassandraCacheDocumentStore}.
 *
 * @author Anıl Şenocak
 */
class CassandraCacheDocumentStoreUnitTests {

	private static final String TABLE_NAME = "test_cache";

	private CqlOperations cqlOperations;
	private CassandraCacheDocumentStore store;

	@BeforeEach
	void setUp() {

		CassandraOperations operations = mock(CassandraOperations.class);
		this.cqlOperations = mock(CqlOperations.class);
		when(operations.getCqlOperations()).thenReturn(this.cqlOperations);

		this.store = new CassandraCacheDocumentStore(operations, TABLE_NAME);
	}

	@Test
	void createsBackingTableOnConstruction() {

		verify(cqlOperations).execute(contains("CREATE TABLE IF NOT EXISTS"));
		verify(cqlOperations).execute(contains('"' + TABLE_NAME + '"'));
	}

	@Test
	void rejectsInvalidTableNames() {

		CassandraOperations operations = mock(CassandraOperations.class);
		when(operations.getCqlOperations()).thenReturn(mock(CqlOperations.class));

		assertThatIllegalArgumentException()
				.isThrownBy(() -> new CassandraCacheDocumentStore(operations, "user_cache-it"));
		assertThatIllegalArgumentException().isThrownBy(() -> new CassandraCacheDocumentStore(operations, "user cache"));
	}

	@Test
	void getReturnsNullForMissingKey() {

		when(cqlOperations.query(anyString(), any(RowMapper.class), any(Object[].class))).thenReturn(List.of());

		assertThat(store.get("users", "missing")).isNull();
	}

	@Test
	void getMapsRowToDocument() {

		CacheDocument document = new CacheDocument("users", "1", "{\"id\":\"1\"}", "{\"username\":\"ada\"}",
				System.currentTimeMillis());

		Row row = row(document);
		stubRows(row);

		CacheDocument result = store.get("users", "1");

		assertThat(result).isEqualTo(document);
	}

	@Test
	void getMapsNullExpirationToNull() {

		CacheDocument document = new CacheDocument("users", "1", "{\"id\":\"1\"}", "{\"username\":\"ada\"}", null);

		Row row = row(document);
		stubRows(row);

		assertThat(store.get("users", "1").expiresAt()).isNull();
	}

	@Test
	void putExecutesInsertWithDocumentColumns() {

		CacheDocument document = new CacheDocument("users", "1", "{\"id\":\"1\"}", "{\"username\":\"ada\"}", 42L);

		store.put(document);

		verify(cqlOperations).execute(contains("INSERT INTO"), eq("users"), eq("1"), eq("{\"id\":\"1\"}"),
				eq("{\"username\":\"ada\"}"), eq(42L));
	}

	@Test
	void deleteRemovesExistingDocumentAndReturnsIt() {

		CacheDocument document = new CacheDocument("users", "1", "{\"id\":\"1\"}", "{\"username\":\"ada\"}", null);

		Row row = row(document);
		stubRows(row);

		assertThat(store.delete("users", "1")).isEqualTo(document);
		verify(cqlOperations).execute(contains("DELETE FROM"), eq("users"), eq("1"));
	}

	@Test
	void deleteDoesNotTouchMissingKey() {

		when(cqlOperations.query(anyString(), any(RowMapper.class), any(Object[].class))).thenReturn(List.of());

		assertThat(store.delete("users", "missing")).isNull();
		verify(cqlOperations, never()).execute(contains("DELETE FROM"), any(), any());
	}

	@Test
	void findAllReturnsEveryDocumentForCacheName() {

		CacheDocument first = new CacheDocument("users", "1", "{\"id\":\"1\"}", "{\"username\":\"ada\"}", null);
		CacheDocument second = new CacheDocument("users", "2", "{\"id\":\"2\"}", "{\"username\":\"grace\"}", null);

		Row firstRow = row(first);
		Row secondRow = row(second);
		stubRows(firstRow, secondRow);

		assertThat(store.findAll("users")).containsExactly(first, second);
	}

	@Test
	void purgeExpiredDeletesOnlyExpiredDocuments() {

		CacheDocument expired = new CacheDocument("users", "1", "{\"id\":\"1\"}", "{\"username\":\"ada\"}", 50L);
		CacheDocument fresh = new CacheDocument("users", "2", "{\"id\":\"2\"}", "{\"username\":\"grace\"}", 200L);

		Row expiredRow = row(expired);
		Row freshRow = row(fresh);
		stubRows(expiredRow, freshRow);

		assertThat(store.purgeExpired("users", 100L)).isEqualTo(1);
		verify(cqlOperations).execute(contains("DELETE FROM"), eq("users"), eq("1"));
		verify(cqlOperations, never()).execute(contains("DELETE FROM"), eq("users"), eq("2"));
	}

	private void stubRows(Row... rows) {

		when(cqlOperations.query(anyString(), any(RowMapper.class), any(Object[].class))).thenAnswer(invocation -> {

			RowMapper<CacheDocument> rowMapper = invocation.getArgument(1);

			List<CacheDocument> result = new ArrayList<>();
			for (int i = 0; i < rows.length; i++) {
				result.add(rowMapper.mapRow(rows[i], i));
			}
			return result;
		});
	}

	private Row row(CacheDocument document) {

		Row row = mock(Row.class);
		when(row.getString("cache_name")).thenReturn(document.cacheName());
		when(row.getString("cache_key")).thenReturn(document.cacheKey());
		when(row.getString("key_json")).thenReturn(document.keyJson());
		when(row.getString("value_json")).thenReturn(document.valueJson());
		when(row.isNull("expires_at")).thenReturn(document.expiresAt() == null);
		if (document.expiresAt() != null) {
			when(row.getLong("expires_at")).thenReturn(document.expiresAt());
		}
		return row;
	}
}
