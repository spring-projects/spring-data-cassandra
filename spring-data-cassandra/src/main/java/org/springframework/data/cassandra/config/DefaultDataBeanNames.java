package org.springframework.data.cassandra.config;

import org.springframework.cassandra.config.xml.DefaultBeanNames;

public interface DefaultDataBeanNames extends DefaultBeanNames {

	public static final String DATA_TEMPLATE = "cassandraTemplate";
	public static final String CONVERTER = "cassandraConverter";
	public static final String MAPPING_CONTEXT = "cassandra-mapping";
}
