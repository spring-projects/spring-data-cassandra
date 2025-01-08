/*
 * Copyright 2016-2025 the original author or authors.
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

import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import com.datastax.oss.driver.api.core.CqlIdentifier;

/**
 * Base value object builder class to construction of user type specifications.
 *
 * @author Fabio J. Mendes
 * @author Mark Paluch
 * @since 1.5
 * @see CqlIdentifier
 */
public abstract class UserTypeNameSpecification {

	/**
	 * User type name.
	 */
	private final @Nullable CqlIdentifier keyspace;
	private final CqlIdentifier name;

	protected UserTypeNameSpecification(CqlIdentifier name) {

		Assert.notNull(name, "Name must not be null");

		this.keyspace = null;
		this.name = name;
	}

	protected UserTypeNameSpecification(@Nullable CqlIdentifier keyspace, CqlIdentifier name) {

		Assert.notNull(name, "Name must not be null");

		this.keyspace = keyspace;
		this.name = name;
	}

	/**
	 * @return the user type keyspace.
	 */
	@Nullable
	public CqlIdentifier getKeyspace() {
		return keyspace;
	}

	/**
	 * @return the user type name.
	 */
	public CqlIdentifier getName() {
		return name;
	}
}
