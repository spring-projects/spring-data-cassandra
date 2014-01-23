package org.springframework.data.cassandra.test.integration.support;

import org.springframework.cassandra.test.integration.support.BuildProperties;

@SuppressWarnings("serial")
public class SpringDataBuildProperties extends BuildProperties {

	public SpringDataBuildProperties() {
		super("/" + SpringDataBuildProperties.class.getName() + ".properties");
	}
}
