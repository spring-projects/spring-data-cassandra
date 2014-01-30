package org.springframework.cassandra.config;

/**
 * Simple data structure to be used when setting the replication factor for a given data center.
 */
public class DataCenterReplication {

	public static DataCenterReplication[] dcrs(DataCenterReplication... dcrs) {
		return dcrs;
	}

	public static DataCenterReplication dcr(String dataCenter, long replicationFactor) {
		return new DataCenterReplication(dataCenter, replicationFactor);
	}

	public String dataCenter;
	public long replicationFactor;

	public DataCenterReplication(String dataCenter, long replicationFactor) {
		this.dataCenter = dataCenter;
		this.replicationFactor = replicationFactor;
	}
}