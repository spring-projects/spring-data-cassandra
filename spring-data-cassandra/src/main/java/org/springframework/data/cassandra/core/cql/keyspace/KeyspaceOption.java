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

import java.util.Map;

import org.springframework.lang.Nullable;

public enum KeyspaceOption implements Option {

	REPLICATION("replication", Map.class, true, false, false),

	DURABLE_WRITES("durable_writes", Boolean.class, false, false, false);

	private Option delegate;

	KeyspaceOption(String name, Class<?> type, boolean requiresValue, boolean escapesValue, boolean quotesValue) {
		this.delegate = new DefaultOption(name, type, requiresValue, escapesValue, quotesValue);
	}

	public Class<?> getType() {
		return delegate.getType();
	}

	public boolean takesValue() {
		return delegate.takesValue();
	}

	public String getName() {
		return delegate.getName();
	}

	public boolean escapesValue() {
		return delegate.escapesValue();
	}

	public boolean quotesValue() {
		return delegate.quotesValue();
	}

	public boolean requiresValue() {
		return delegate.requiresValue();
	}

	public void checkValue(Object value) {
		delegate.checkValue(value);
	}

	public boolean isCoerceable(Object value) {
		return delegate.isCoerceable(value);
	}

	public String toString() {
		return delegate.toString();
	}

	public String toString(@Nullable Object value) {
		return delegate.toString(value);
	}

	/**
	 * Known Replication Strategy options.
	 *
	 * @author John McPeek
	 */
	public enum ReplicationStrategy {
		SIMPLE_STRATEGY("SimpleStrategy"), NETWORK_TOPOLOGY_STRATEGY("NetworkTopologyStrategy");

		private String value;

		ReplicationStrategy(String value) {
			this.value = value;
		}

		public String getValue() {
			return value;
		}

		public String toString() {
			return getValue();
		}
	}
}
