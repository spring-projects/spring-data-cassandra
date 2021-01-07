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

import com.datastax.oss.driver.api.core.CqlIdentifier;

import org.springframework.lang.Nullable;

/**
 * Describes an index.
 *
 * @author Matthew T. Adams
 * @author David Webb
 */
public interface IndexDescriptor {

	/**
	 * Returns the name of the index.
	 */
	@Nullable
	CqlIdentifier getName();

	/**
	 * Returns the table name for the index
	 */
	CqlIdentifier getTableName();

	CqlIdentifier getColumnName();

	@Nullable
	String getUsing();

	boolean isCustom();
}
