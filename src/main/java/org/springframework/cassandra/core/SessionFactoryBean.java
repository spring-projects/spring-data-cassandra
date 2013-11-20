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

import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.data.cassandra.core.Keyspace;

import com.datastax.driver.core.Session;

/**
 * @author David Webb
 * 
 */
public class SessionFactoryBean implements FactoryBean<Session>, InitializingBean {

	private Keyspace keyspace;

	public SessionFactoryBean() {
	}

	public SessionFactoryBean(Keyspace keyspace) {
		setKeyspace(keyspace);
	}

	/* (non-Javadoc)
	 * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 */
	@Override
	public void afterPropertiesSet() throws Exception {
		if (keyspace == null) {
			throw new IllegalStateException("Keyspace required.");
		}
	}

	/**
	 * @return Returns the keyspace.
	 */
	public Keyspace getKeyspace() {
		return keyspace;
	}

	/**
	 * @param keyspace The keyspace to set.
	 */
	public void setKeyspace(Keyspace keyspace) {
		this.keyspace = keyspace;
	}

	/* (non-Javadoc)
	 * @see org.springframework.beans.factory.FactoryBean#getObject()
	 */
	@Override
	public Session getObject() {
		return keyspace.getSession();
	}

	/* (non-Javadoc)
	 * @see org.springframework.beans.factory.FactoryBean#getObjectType()
	 */
	@Override
	public Class<?> getObjectType() {
		return Session.class;
	}

	/* (non-Javadoc)
	 * @see org.springframework.beans.factory.FactoryBean#isSingleton()
	 */
	@Override
	public boolean isSingleton() {
		return true;
	}

}
