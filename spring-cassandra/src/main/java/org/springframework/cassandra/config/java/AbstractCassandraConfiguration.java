/*
 * Copyright 2011-2012 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.cassandra.config.java;

import org.springframework.cassandra.core.CassandraOperations;
import org.springframework.cassandra.core.CassandraTemplate;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

/**
 * Base class for Spring Cassandra configuration using JavaConfig.
 * 
 * @author Alex Shvid
 * @author Matthew T. Adams
 */
@Configuration
public abstract class AbstractCassandraConfiguration {

	/**
	 * The name of the keyspace to connect to. If {@literal null} or empty, then the system keyspace will be used.
	 */
	protected abstract String getKeyspace();

	/**
	 * The {@link Cluster} instance to connect to. Must not be null.
	 */
	@Bean
	public abstract Cluster cluster();

	/**
	 * Creates a {@link Session} using the {@link Cluster} instance configured in {@link #cluster()}.
	 * 
	 * @see #cluster()
	 */
	@Bean
	public Session session() {
		String keyspace = getKeyspace();
		if (StringUtils.hasText(keyspace)) {
			return cluster().connect(keyspace);
		} else {
			return cluster().connect();
		}
	}

	/**
	 * A {@link CassandraTemplate} created from the {@link Session} returned by {@link #session()}.
	 */
	@Bean
	public CassandraOperations cassandraTemplate() {
		return new CassandraTemplate(session());
	}
}
