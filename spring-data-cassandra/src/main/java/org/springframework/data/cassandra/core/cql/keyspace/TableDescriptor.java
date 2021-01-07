/*
 * Copyright 2013-2021 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.core.cql.keyspace;

import java.util.List;
import java.util.Map;

import com.datastax.oss.driver.api.core.CqlIdentifier;

/**
 * Describes a table.
 *
 * @author Matthew T. Adams
 * @author Alex Shvid
 */
public interface TableDescriptor {

	/**
	 * Returns the name of the table.
	 */
	CqlIdentifier getName();

	/**
	 * Returns an unmodifiable {@link List} of {@link ColumnSpecification}s.
	 */
	List<ColumnSpecification> getColumns();

	/**
	 * Returns an unmodifiable list of all partition key columns.
	 */
	List<ColumnSpecification> getPartitionKeyColumns();

	/**
	 * Returns an unmodifiable list of all primary key columns that are not also partition key columns.
	 */
	List<ColumnSpecification> getClusteredKeyColumns();

	/**
	 * Returns an unmodifiable list of all partition and primary key columns.
	 */
	List<ColumnSpecification> getPrimaryKeyColumns();

	/**
	 * Returns an unmodifiable list of all non-key columns.
	 */
	List<ColumnSpecification> getNonKeyColumns();

	/**
	 * Returns an unmodifiable {@link Map} of table options.
	 */
	Map<String, Object> getOptions();
}
