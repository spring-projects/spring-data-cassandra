package org.springframework.data.cassandra.test.config;

import lombok.extern.log4j.Log4j;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.cassandra.core.KeyspaceFactoryBean;
import org.springframework.data.cassandra.core.CassandraThriftTemplate;

/**
 * Setup any spring configuration for unit tests
 * 
 * @author David Webb
 *
 */
@Log4j
@Configuration
public class TestConfig {
	
	public @Bean KeyspaceFactoryBean cassandra() {
		KeyspaceFactoryBean cfb = new KeyspaceFactoryBean();
		cfb.setHost("127.0.0.1");
		cfb.setPort(9160);
		cfb.setKeyspaceName("test_keyspace");
		
		return cfb;
	}
	
	public @Bean CassandraThriftTemplate cassandraTemplate() {
		CassandraThriftTemplate template = null;
		try {
			template = new CassandraThriftTemplate(cassandra().getObject());
		} catch (Exception e) {
			log.error(e);
		}
		return template;
	}


}
