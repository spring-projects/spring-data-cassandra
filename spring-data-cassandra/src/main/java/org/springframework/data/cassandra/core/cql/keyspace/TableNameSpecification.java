/*
 * Copyright 2013-present the original author or authors.
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

import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import com.datastax.oss.driver.api.core.CqlIdentifier;

/**
 * Abstract builder class to support the construction of table specifications.
 *
 * @author Matthew T. Adams
 * @author Mark Paluch
 */
public abstract class TableNameSpecification {

	/**
	 * The keyspace of the table.
	 */

	private final @Nullable CqlIdentifier keyspace;

	/**
	 * The name of the table.
	 */
	private final CqlIdentifier name;

	protected TableNameSpecification(CqlIdentifier name) {

		Assert.notNull(name, "CqlIdentifier must not be null");

		this.keyspace = null;
		this.name = name;
	}

	protected TableNameSpecification(@Nullable CqlIdentifier keyspace, CqlIdentifier name) {

		Assert.notNull(name, "CqlIdentifier must not be null");

		this.keyspace = keyspace;
		this.name = name;
	}

	@Nullable
	public CqlIdentifier getKeyspace() {
		return keyspace;
	}

	public CqlIdentifier getName() {
		return this.name;
	}

}
