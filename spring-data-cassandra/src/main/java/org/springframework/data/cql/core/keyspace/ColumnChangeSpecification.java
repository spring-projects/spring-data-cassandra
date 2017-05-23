/*
 * Copyright 2013-2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cql.core.keyspace;

import static org.springframework.data.cql.core.CqlIdentifier.*;

import org.springframework.data.cql.core.CqlIdentifier;
import org.springframework.util.Assert;

/**
 * Base value object class for column change specifications.
 *
 * @author Matthew T. Adams
 * @author Mark Paluch
 */
public abstract class ColumnChangeSpecification {

	protected CqlIdentifier name;

	/**
	 * Create a new {@link ColumnChangeSpecification}.
	 *
	 * @param name must not be empty or {@literal null}.
	 */
	protected ColumnChangeSpecification(String name) {
		this(cqlId(name));
	}

	/**
	 * Create a new {@link ColumnChangeSpecification}.
	 *
	 * @param name must not be {@literal null}.
	 */
	protected ColumnChangeSpecification(CqlIdentifier name) {
		setName(name);
	}

	/**
	 * Sets the column name.
	 *
	 * @param name must not be {@literal null}.
	 */
	protected void setName(CqlIdentifier name) {

		Assert.notNull(name, "Name must not be null");
		this.name = name;
	}

	/**
	 * @return the column name.
	 */
	public CqlIdentifier getName() {
		return name;
	}
}
