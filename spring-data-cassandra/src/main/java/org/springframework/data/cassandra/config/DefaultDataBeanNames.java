package org.springframework.data.cassandra.config;

import org.springframework.cassandra.config.xml.DefaultBeanNames;

public interface DefaultDataBeanNames extends DefaultBeanNames {

	public static final String DATA_TEMPLATE = "cassandra-template";
	public static final String CONVERTER = "cassandra-converter";
}
