/*
 * Copyright 2011-2013 the original author or authors.
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
package org.springframework.cassandra.core;

import com.datastax.driver.core.Session;

/**
 * Simple Cassandra Keyspace object
 * 
 * @author Alex Shvid
 */
public class Keyspace {

	private final String keyspace;
	private final Session session;

	/**
	 * Constructor used for a basic keyspace configuration
	 * 
	 * @param keyspace, system if {@literal null}.
	 * @param session must not be {@literal null}.
	 * @param cassandraConverter must not be {@literal null}.
	 */
	public Keyspace(String keyspace, Session session) {
		this.keyspace = keyspace;
		this.session = session;
	}

	public String getKeyspace() {
		return keyspace;
	}

	public Session getSession() {
		return session;
	}
}
