package org.springframework.cassandra.core.keyspace;

import java.util.Map;

public enum KeyspaceOption implements Option {
	REPLICATION("replication", Map.class, true, false, false),

	DURABLE_WRITES("durable_writes", Boolean.class, false, false, false);

	private Option delegate;

	private KeyspaceOption(String name, Class<?> type, boolean requiresValue, boolean escapesValue, boolean quotesValue) {
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

	public String toString(Object value) {
		return delegate.toString(value);
	}

	/**
	 * Known Replication Strategy options.
	 * 
	 * @author John McPeek
	 * 
	 */
	public enum ReplicationStrategy {
		SIMPLE_STRATEGY("SimpleStrategy"), NETWORK_TOPOLOGY_STRATEGY("NetworkTopologyStrategy");

		private String value;

		private ReplicationStrategy(String value) {
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
