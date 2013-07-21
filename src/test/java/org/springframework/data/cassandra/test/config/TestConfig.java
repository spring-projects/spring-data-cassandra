package org.springframework.data.cassandra.test.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.cassandra.core.CassandraFactoryBean;
import org.springframework.data.cassandra.core.CassandraThriftTemplate;

/**
 * Setup any spring configuration for unit tests
 * 
 * @author David Webb
 *
 */
@Configuration
public class TestConfig {
	
	public @Bean CassandraFactoryBean cassandra() {
		CassandraFactoryBean cfb = new CassandraFactoryBean();
		cfb.setHost("127.0.0.1");
		cfb.setPort(9160);
		cfb.setKeyspaceName("test_keyspace");
		
		return cfb;
	}
	
	public @Bean CassandraThriftTemplate cassandraTemplate() {
		CassandraThriftTemplate template = new CassandraThriftTemplate(cassandra());
		return template;
	}


}
