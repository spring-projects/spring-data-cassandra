/*
 * Copyright 2016-2021 the original author or authors.
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
package org.springframework.data.cassandra.core.cql.keyspace;

import com.datastax.oss.driver.api.core.CqlIdentifier;

import org.springframework.util.Assert;

/**
 * {@link ColumnChangeSpecification} to rename a column.
 *
 * @author Mark Paluch
 * @see ColumnChangeSpecification
 * @since 1.5
 */
public class RenameColumnSpecification extends ColumnChangeSpecification {

	private final CqlIdentifier targetName;

	/**
	 * Create a new {@link ColumnChangeSpecification}.
	 *
	 * @param from must not be {@literal null}.
	 * @param to must not be {@literal null}.
	 */
	RenameColumnSpecification(CqlIdentifier from, CqlIdentifier to) {

		super(from);

		Assert.notNull(to, "Target name must not be null");

		this.targetName = to;
	}

	/**
	 * @return the column name.
	 */
	public CqlIdentifier getTargetName() {
		return targetName;
	}
}
