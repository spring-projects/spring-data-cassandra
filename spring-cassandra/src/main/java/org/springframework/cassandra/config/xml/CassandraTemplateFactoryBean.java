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
package org.springframework.cassandra.config.xml;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.cassandra.core.CassandraTemplate;

import com.datastax.driver.core.Session;

/**
 * Factory for configuring a {@link CassandraTemplate}.
 * 
 * @author Matthew T. Adams
 */

public class CassandraTemplateFactoryBean implements FactoryBean<CassandraTemplate>, InitializingBean {

	private static final Logger log = LoggerFactory.getLogger(CassandraTemplateFactoryBean.class);

	private CassandraTemplate template;
	private Session session;

	public CassandraTemplate getObject() {
		return template;
	}

	public Class<? extends CassandraTemplate> getObjectType() {
		return CassandraTemplate.class;
	}

	public boolean isSingleton() {
		return true;
	}

	public void afterPropertiesSet() throws Exception {

		if (session == null) {
			throw new IllegalStateException("session is required");
		}

		this.template = new CassandraTemplate(session);
	}

	public void setSession(Session session) {
		this.session = session;
	}
}
