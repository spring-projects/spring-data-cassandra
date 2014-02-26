/*
 * Copyright 2013-2014 the original author or authors
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.config;

import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.data.cassandra.convert.CassandraConverter;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.core.CassandraTemplate;
import org.springframework.util.Assert;

import com.datastax.driver.core.Session;

public class CassandraTemplateFactoryBean implements FactoryBean<CassandraOperations>, InitializingBean {

	protected Session session;
	protected CassandraConverter converter;

	@Override
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(session);
		Assert.notNull(converter);
	}

	@Override
	public CassandraOperations getObject() throws Exception {
		return new CassandraTemplate(session, converter);
	}

	@Override
	public Class<?> getObjectType() {
		return CassandraOperations.class;
	}

	@Override
	public boolean isSingleton() {
		return true;
	}

	public void setSession(Session session) {
		Assert.notNull(session);
		this.session = session;
	}

	public void setConverter(CassandraConverter converter) {
		Assert.notNull(converter);
		this.converter = converter;
	}
}
