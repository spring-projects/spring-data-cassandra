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

import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

import com.datastax.oss.driver.api.core.CqlIdentifier;

/**
 * Base value object to support the construction of keyspace specifications.
 *
 * @author John McPeek
 * @author David Webb
 * @param <T> The subtype of the {@link KeyspaceActionSpecification}
 */
public abstract class KeyspaceActionSpecification {

	/**
	 * The name of the keyspace.
	 */
	private final CqlIdentifier name;

	protected KeyspaceActionSpecification(CqlIdentifier name) {

		Assert.notNull(name, "CqlIdentifier must not be null");

		this.name = name;
	}

	public CqlIdentifier getName() {
		return name;
	}

	protected boolean canEqual(final Object other) {
		return other instanceof KeyspaceActionSpecification;
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

		if (!(o instanceof KeyspaceActionSpecification)) {
			return false;
		}

		KeyspaceActionSpecification that = (KeyspaceActionSpecification) o;
		return ObjectUtils.nullSafeEquals(name, that.name);
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public int hashCode() {
		return ObjectUtils.nullSafeHashCode(name);
	}
}
