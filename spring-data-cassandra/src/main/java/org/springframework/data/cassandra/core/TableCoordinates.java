/*
 * Copyright 2017-2020 the original author or authors.
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
package org.springframework.data.cassandra.core;

import java.util.Optional;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import org.springframework.data.cassandra.core.mapping.BasicCassandraPersistentEntity;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentEntity;

/**
 * The coordinates that are defining optional keyspace and non-optional table. They will be used when constructing and
 * executing CQL queries.
 * 
 * @author Tomasz Lelek
 */
public class TableCoordinates {
	@SuppressWarnings("OptionalUsedAsFieldOrParameterType") private final Optional<CqlIdentifier> keyspaceName;
	private final CqlIdentifier tableName;

	@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
	private TableCoordinates(Optional<CqlIdentifier> keyspaceName, CqlIdentifier tableName) {
		this.keyspaceName = keyspaceName;
		this.tableName = tableName;
	}

	@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
	public static TableCoordinates of(Optional<CqlIdentifier> keyspaceName, CqlIdentifier tableName) {
		return new TableCoordinates(keyspaceName, tableName);
	}

	public static TableCoordinates of(BasicCassandraPersistentEntity<?> persistentEntity) {
		return new TableCoordinates(persistentEntity.getKeyspaceName(), persistentEntity.getTableName());
	}

	public static TableCoordinates of(CassandraPersistentEntity<?> persistentEntity) {
		return new TableCoordinates(persistentEntity.getKeyspaceName(), persistentEntity.getTableName());
	}

	public Optional<CqlIdentifier> getKeyspaceName() {
		return keyspaceName;
	}

	public CqlIdentifier getTableName() {
		return tableName;
	}

}
