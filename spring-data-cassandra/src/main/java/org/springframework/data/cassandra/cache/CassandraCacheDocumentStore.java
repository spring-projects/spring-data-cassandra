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
import java.util.Objects;

import org.jspecify.annotations.Nullable;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.core.cql.CqlOperations;
import org.springframework.data.cassandra.core.cql.RowMapper;

import com.datastax.oss.driver.api.core.cql.Row;

/**
 * {@link CacheDocumentStore} implementation backed by Cassandra. Documents are stored in a single table whose
 * primary key is {@code (cache_name, cache_key)} so that repeated writes for the same key are naturally idempotent.
 *
 * @author Anıl Şenocak
 * @since 5.2
 */
public final class CassandraCacheDocumentStore implements CacheDocumentStore {
	private final CqlOperations cqlOperations;
	private final String tableName;

	public CassandraCacheDocumentStore(final CassandraOperations operations) {
		this(operations, CassandraCacheImpl.DEFAULT_TABLE_NAME);
	}

	public CassandraCacheDocumentStore(final CassandraOperations operations, final String tableName) {
		Objects.requireNonNull(operations, "Operations must not be null");
		this.cqlOperations = operations.getCqlOperations();
		this.tableName = validateTableName(tableName);
		ensureTableExists();
	}

	@Override
	public @Nullable CacheDocument get(final String cacheName, final String cacheKey) {
		Objects.requireNonNull(cacheName, "CacheName must not be null");
		Objects.requireNonNull(cacheKey, "CacheKey must not be null");
		final List<CacheDocument> documents = cqlOperations.query(selectCql(), ROW_MAPPER, cacheName, cacheKey);
		return documents.isEmpty() ? null : documents.get(0);
	}

	@Override
	public void put(final CacheDocument document) {
		Objects.requireNonNull(document, "Document must not be null");
		cqlOperations.execute(insertCql(), document.cacheName(), document.cacheKey(), document.keyJson(),
				document.valueJson(), document.expiresAt());
	}

	@Override
	public @Nullable CacheDocument delete(final String cacheName, final String cacheKey) {
		Objects.requireNonNull(cacheName, "CacheName must not be null");
		Objects.requireNonNull(cacheKey, "CacheKey must not be null");
		final CacheDocument document = get(cacheName, cacheKey);
		if (document != null) {
			cqlOperations.execute(deleteCql(), cacheName, cacheKey);
		}
		return document;
	}

	@Override
	public List<CacheDocument> findAll(final String cacheName) {
		Objects.requireNonNull(cacheName, "CacheName must not be null");
		return cqlOperations.query(findAllCql(), ROW_MAPPER, cacheName);
	}

	@Override
	public int purgeExpired(final String cacheName, final long nowMillis) {
		int purged = 0;
		for (CacheDocument document : findAll(cacheName)) {
			if (document.expiresAt() != null && document.expiresAt() <= nowMillis) {
				delete(cacheName, document.cacheKey());
				purged++;
			}
		}
		return purged;
	}

	private String selectCql() {
		return """
				SELECT cache_name, cache_key, key_json, value_json, expires_at
				FROM "%s" WHERE cache_name = ? AND cache_key = ?
				""".formatted(tableName);
	}

	private String insertCql() {
		return """
				INSERT INTO "%s" (cache_name, cache_key, key_json, value_json, expires_at)
				VALUES (?, ?, ?, ?, ?)
				""".formatted(tableName);
	}

	private String deleteCql() {
		return """
				DELETE FROM "%s" WHERE cache_name = ? AND cache_key = ?
				""".formatted(tableName);
	}

	private String findAllCql() {
		return """
				SELECT cache_name, cache_key, key_json, value_json, expires_at
				FROM "%s" WHERE cache_name = ?
				""".formatted(tableName);
	}

	private void ensureTableExists() {
		final String cql = """
				CREATE TABLE IF NOT EXISTS "%s" (
					cache_name text,
					cache_key text,
					key_json text,
					value_json text,
					expires_at bigint,
					PRIMARY KEY (cache_name, cache_key)
				)
				""".formatted(tableName);
		try {
			cqlOperations.execute(cql);
		} catch (RuntimeException exception) {
			// Ignore; the schema may already be managed by the application or a schema-action setting.
		}
	}

	private static String validateTableName(final String tableName) {
		Objects.requireNonNull(tableName, "TableName must not be null");
		if (!tableName.matches("[A-Za-z0-9_]+")) {
			throw new IllegalArgumentException(
					"Table name '%s' is not a valid Cassandra table identifier; use only alphanumeric and underscore characters"
							.formatted(tableName));
		}
		return tableName;
	}

	private static final RowMapper<CacheDocument> ROW_MAPPER = (row, rowNum) -> toDocument(row);

	private static CacheDocument toDocument(final Row row) {
		return new CacheDocument(row.getString("cache_name"), row.getString("cache_key"), row.getString("key_json"),
				row.getString("value_json"), row.isNull("expires_at") ? null : row.getLong("expires_at"));
	}
}
