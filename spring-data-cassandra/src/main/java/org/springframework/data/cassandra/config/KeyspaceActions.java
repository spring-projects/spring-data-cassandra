/*
 * Copyright 2017-2021 the original author or authors.
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
package org.springframework.data.cassandra.config;

import java.util.Arrays;
import java.util.List;

import org.springframework.data.cassandra.core.cql.keyspace.KeyspaceActionSpecification;
import org.springframework.util.ObjectUtils;

/**
 * Collection of {@link KeyspaceActionSpecification}s. Wraps none, one or multiple keyspace actions (creates, drops).
 *
 * @author Mark Paluch
 * @since 2.0
 */
public final class KeyspaceActions {

	private final List<KeyspaceActionSpecification> actions;

	public KeyspaceActions(KeyspaceActionSpecification... actions) {
		this(Arrays.asList(actions));
	}

	public KeyspaceActions(List<KeyspaceActionSpecification> actions) {
		this.actions = actions;
	}

	public List<KeyspaceActionSpecification> getActions() {
		return this.actions;
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

		if (!(o instanceof KeyspaceActions)) {
			return false;
		}

		KeyspaceActions that = (KeyspaceActions) o;
		return ObjectUtils.nullSafeEquals(actions, that.actions);
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public int hashCode() {
		return ObjectUtils.nullSafeHashCode(actions);
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "KeyspaceActions(actions=" + this.getActions() + ")";
	}
}
