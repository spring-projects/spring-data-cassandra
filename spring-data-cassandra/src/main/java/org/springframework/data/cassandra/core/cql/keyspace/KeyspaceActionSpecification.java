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
package org.springframework.data.cassandra.core.cql.keyspace;

import static org.springframework.data.cassandra.core.cql.KeyspaceIdentifier.*;

import org.springframework.data.cassandra.core.cql.KeyspaceIdentifier;
import org.springframework.util.Assert;

/**
 * Abstract builder class to support the construction of keyspace specifications.
 *
 * @author John McPeek
 * @author David Webb
 * @param <T> The subtype of the {@link KeyspaceActionSpecification}
 */
public abstract class KeyspaceActionSpecification<T extends KeyspaceActionSpecification<T>> {

	/**
	 * The name of the keyspace.
	 */
	private KeyspaceIdentifier name;

	/**
	 * Sets the keyspace name.
	 *
	 * @return this
	 */
	public T name(String name) {
		return name(ksId(name));
	}

	/**
	 * Sets the keyspace name.
	 *
	 * @return this
	 */
	@SuppressWarnings("unchecked")
	public T name(KeyspaceIdentifier name) {

		Assert.notNull(name, "KeyspaceIdentifier must not be null");

		this.name = name;
		return (T) this;
	}

	public KeyspaceIdentifier getName() {
		return name;
	}

	/**
	 * Equality incorporates the exact type to distinguish between instances based on this type.
	 *
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object o) {

		if (this == o)
			return true;
		if (!(o instanceof KeyspaceActionSpecification) || !getClass().equals(o.getClass()))
			return false;

		KeyspaceActionSpecification<?> that = (KeyspaceActionSpecification<?>) o;

		return name != null ? name.equals(that.name) : that.name == null;
	}

	/**
	 * Hash code incorporates the exact type to distinguish between instances based on this type.
	 *
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {

		int result = name != null ? name.hashCode() : 0;
		result = 31 * result + (getClass().hashCode());
		return result;
	}
}
