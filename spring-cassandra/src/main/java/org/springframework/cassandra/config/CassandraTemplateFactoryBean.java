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
package org.springframework.cassandra.config;

import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.cassandra.core.CqlOperations;
import org.springframework.cassandra.core.CqlTemplate;

import com.datastax.driver.core.Session;

/**
 * Factory for configuring a {@link CqlTemplate}.
 * 
 * @author Matthew T. Adams
 */
public class CassandraTemplateFactoryBean implements FactoryBean<CqlOperations>, InitializingBean {

	private CqlTemplate template;
	private Session session;

	@Override
	public CqlOperations getObject() {
		return template;
	}

	@Override
	public Class<? extends CqlOperations> getObjectType() {
		return CqlOperations.class;
	}

	@Override
	public boolean isSingleton() {
		return true;
	}

	@Override
	public void afterPropertiesSet() throws Exception {

		if (session == null) {
			throw new IllegalStateException("session is required");
		}

		this.template = new CqlTemplate(session);
	}

	public void setSession(Session session) {
		this.session = session;
	}
}
