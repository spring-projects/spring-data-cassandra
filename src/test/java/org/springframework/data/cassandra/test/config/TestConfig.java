package org.springframework.data.cassandra.test.config;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import lombok.extern.log4j.Log4j;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.cassandra.core.CassandraConnectionFactoryBean;
import org.springframework.data.cassandra.core.CassandraTemplate;

/**
 * Setup any spring configuration for unit tests
 * 
 * @author David Webb
 *
 */
@Log4j
@Configuration
public class TestConfig {
	
	public @Bean CassandraConnectionFactoryBean cassandra() {

		List<String> seeds = new ArrayList<String>();
		seeds.add("127.0.0.1");
		
		CassandraConnectionFactoryBean cfb = new CassandraConnectionFactoryBean();
		
		cfb.setSeeds(seeds);
		cfb.setPort(9042);
		
		return cfb;
	}
	
	public @Bean CassandraTemplate cassandraTemplate() {
		CassandraTemplate template = null;
		try {
			template = new CassandraTemplate(cassandra().getObject());
		} catch (Exception e) {
			log.error(e);
		}
		return template;
	}


}
