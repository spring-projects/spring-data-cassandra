/*
 * Copyright 2017-2020 the original author or authors.
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

/**
 * Value object representing replication factor for a given data center.
 *
 * @author Mark Paluch
 */
public class DataCenterReplication {

	private final String dataCenter;

	private final long replicationFactor;

	private DataCenterReplication(String dataCenter, long replicationFactor) {

		this.dataCenter = dataCenter;
		this.replicationFactor = replicationFactor;
	}

	/**
	 * Creates a new {@link DataCenterReplication} given {@code dataCenter} and {@code replicationFactor}.
	 *
	 * @param dataCenter must not be {@literal null}.
	 * @param replicationFactor the replication factor.
	 * @return {@link DataCenterReplication} for {@code dataCenter} and {@code replicationFactor}.
	 */
	public static DataCenterReplication of(String dataCenter, long replicationFactor) {
		return new DataCenterReplication(dataCenter, replicationFactor);
	}

	/**
	 * @return the data center.
	 */
	public String getDataCenter() {
		return dataCenter;
	}

	/**
	 * @return the replication factor.
	 */
	public long getReplicationFactor() {
		return replicationFactor;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "DataCenterReplication(dataCenter=" + this.getDataCenter() + ", replicationFactor="
				+ this.getReplicationFactor() + ")";
	}
}
