/*
 * Copyright 2010-2012 the original author or authors.
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
package org.springframework.data.cassandra.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.core.io.ResourceLoader;
import org.springframework.data.cassandra.core.convert.CassandraConverter;

/**
 * Primary implementation of {@link MongoOperations}.
 * 
 * @author Brian O'Neill
 */
public class CassandraTemplate {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(CassandraTemplate.class);
	private CassandraFactoryBean cassandraFactory;
	private CassandraConverter converter;
	
	private ApplicationEventPublisher eventPublisher;
	private ResourceLoader resourceLoader;

	/**
	 * Constructor used for a basic template configuration
	 * 
	 * @param cassandraFactory
	 */
	public CassandraTemplate(CassandraFactoryBean cassandraFactory) {
		this(cassandraFactory, null);
	}

	/**
	 * Constructor used for a basic template configuration.
	 * 
	 * @param mongoDbFactory
	 * @param mongoConverter
	 */
	public CassandraTemplate(CassandraFactoryBean cassandraFactory,
			CassandraConverter converter) {
		this.cassandraFactory = cassandraFactory;
		this.converter = converter == null ? getDefaultConverter(cassandraFactory) : converter;
	}

	/**
	 * Returns the default
	 * {@link org.springframework.data.cassandra.core.convert.CassandraConverter}
	 * .
	 * 
	 * @return
	 */
	public CassandraConverter getConverter() {
		return this.converter;
	}

	/**
	 * Returns the default
	 * {@link org.springframework.data.cassandra.core.convert.CassandraConverter}
	 * .
	 * 
	 * @return
	 */
	public CassandraConverter getDefaultConverter(CassandraFactoryBean cassandraFactory) {
		return this.converter;
	}
}
