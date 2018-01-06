/*
 * Copyright 2013-2018 the original author or authors.
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

import lombok.EqualsAndHashCode;

import org.springframework.data.cassandra.core.cql.KeyspaceIdentifier;
import org.springframework.util.Assert;

/**
 * Base value object to support the construction of keyspace specifications.
 *
 * @author John McPeek
 * @author David Webb
 * @param <T> The subtype of the {@link KeyspaceActionSpecification}
 */
@EqualsAndHashCode
public abstract class KeyspaceActionSpecification {

	/**
	 * The name of the keyspace.
	 */
	private final KeyspaceIdentifier name;

	protected KeyspaceActionSpecification(KeyspaceIdentifier name) {

		Assert.notNull(name, "KeyspaceIdentifier must not be null");

		this.name = name;
	}

	public KeyspaceIdentifier getName() {
		return name;
	}
}
