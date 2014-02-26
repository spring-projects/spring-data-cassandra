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

import com.datastax.driver.core.Cluster;

/**
 * Base class for Spring Cassandra configuration that can handle creating namespaces, execute arbitrary CQL on startup &
 * shutdown, and optionally drop namespaces.
 * 
 * @author Matthew T. Adams
 */
@Configuration
public abstract class AbstractSessionConfiguration extends AbstractClusterConfiguration {

	protected abstract String getKeyspaceName();

	@Bean
	public CassandraCqlSessionFactoryBean session() throws Exception {

		Cluster cluster = cluster().getObject();

		CassandraCqlSessionFactoryBean bean = new CassandraCqlSessionFactoryBean();
		bean.setCluster(cluster);
		bean.setKeyspaceName(getKeyspaceName());

		return bean;
	}
}
