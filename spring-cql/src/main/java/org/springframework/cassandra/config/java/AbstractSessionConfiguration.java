/*
 * Copyright 2013-2014 the original author or authors.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.cassandra.config.java;

import org.springframework.cassandra.config.CassandraCqlSessionFactoryBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Spring {@link @Configuration} class used to configure a Cassandra client application
 * {@link com.datastax.driver.core.Session} connected to a Cassandra {@link com.datastax.driver.core.Cluster}.
 *
 * Enables a Cassandra Keyspace to be specified along with the ability to execute arbitrary CQL on startup
 * as well as shutdown.
 *
 * @author Matthew T. Adams
 * @author John Blum
 * @see org.springframework.cassandra.config.java.AbstractClusterConfiguration
 * @see org.springframework.context.annotation.Configuration
 */
@Configuration
public abstract class AbstractSessionConfiguration extends AbstractClusterConfiguration {

	@Bean
	public CassandraCqlSessionFactoryBean session() throws Exception {

		CassandraCqlSessionFactoryBean bean = new CassandraCqlSessionFactoryBean();

		bean.setCluster(cluster().getObject());
		bean.setKeyspaceName(getKeyspaceName());

		return bean;
	}

	protected abstract String getKeyspaceName();

}
