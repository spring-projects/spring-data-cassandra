package org.springframework.data.cassandra.test.integration.support;

import org.springframework.cassandra.test.integration.support.SpringCassandraBuildProperties;

@SuppressWarnings("serial")
public class SpringDataCassandraBuildProperties extends SpringCassandraBuildProperties {

	public SpringDataCassandraBuildProperties() {
		super("/" + SpringDataCassandraBuildProperties.class.getName() + ".properties");
	}
}
