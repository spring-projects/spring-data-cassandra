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

import org.springframework.data.cassandra.core.cql.KeyspaceIdentifier;

import com.datastax.oss.driver.api.core.CqlIdentifier;

/**
 * Object to configure a {@code DROP KEYSPACE} specification.
 *
 * @author Mark Paluch
 */
public class DropKeyspaceSpecification extends KeyspaceActionSpecification {

	private boolean ifExists;

	private DropKeyspaceSpecification(CqlIdentifier name) {
		super(name);
	}

	/**
	 * Create a new {@link DropKeyspaceSpecification} for the given {@code name}.
	 *
	 * @param name must not be {@literal null} or empty.
	 * @return a new {@link DropKeyspaceSpecification}.
	 */
	public static DropKeyspaceSpecification dropKeyspace(String name) {
		return dropKeyspace(CqlIdentifier.fromCql(name));
	}

	/**
	 * Create a new {@link DropKeyspaceSpecification} for the given {@code name}.
	 *
	 * @param name must not be {@literal null}.
	 * @return a new {@link DropKeyspaceSpecification}.
	 * @deprecated since 3.0, use {@link #dropKeyspace(CqlIdentifier)}.
	 */
	@Deprecated
	public static DropKeyspaceSpecification dropKeyspace(KeyspaceIdentifier name) {
		return dropKeyspace(CqlIdentifier.fromCql(name.toCql()));
	}

	/**
	 * Create a new {@link DropKeyspaceSpecification} for the given {@code name}.
	 *
	 * @param name must not be {@literal null}.
	 * @return a new {@link DropKeyspaceSpecification}.
	 * @since 3.0
	 */
	public static DropKeyspaceSpecification dropKeyspace(CqlIdentifier name) {
		return new DropKeyspaceSpecification(name);
	}

	/**
	 * Causes the inclusion of an {@code IF EXISTS} clause.
	 *
	 * @return this
	 */
	public DropKeyspaceSpecification ifExists() {
		return ifExists(true);
	}

	/**
	 * Toggles the inclusion of an {@code IF EXISTS} clause.
	 *
	 * @return this
	 */
	public DropKeyspaceSpecification ifExists(boolean ifExists) {

		this.ifExists = ifExists;

		return this;
	}

	public boolean getIfExists() {
		return ifExists;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object o) {

		if (this == o) {
			return true;
		}

		if (!(o instanceof DropKeyspaceSpecification)) {
			return false;
		}

		if (!super.equals(o)) {
			return false;
		}

		DropKeyspaceSpecification that = (DropKeyspaceSpecification) o;
		return ifExists == that.ifExists;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public int hashCode() {
		int result = super.hashCode();
		result = 31 * result + (ifExists ? 1 : 0);
		return result;
	}
}
