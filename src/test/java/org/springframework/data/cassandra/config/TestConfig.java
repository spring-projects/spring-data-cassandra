package org.springframework.data.cassandra.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;

/**
 * Setup any spring configuration for unit tests
 * 
 * @author David Webb
 *
 */
@Configuration
public class TestConfig extends AbstractCassandraConfiguration {
	
	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.config.AbstractCassandraConfiguration#getKeyspaceName()
	 */
	@Override
	protected String getKeyspaceName() {
		return null;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.config.AbstractCassandraConfiguration#cluster()
	 */
	@Override
	@Bean
	public Cluster cluster() throws Exception {
		
		Builder builder = Cluster.builder();
		
		builder.addContactPoint("127.0.0.1");
		
		return builder.build();
	}
	
}
