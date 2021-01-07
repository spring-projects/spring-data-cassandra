/*
 * Copyright 2013-2021 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
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
import org.springframework.data.cassandra.SessionFactory;
import org.springframework.data.cassandra.core.cql.CqlTemplate;
import org.springframework.data.cassandra.core.cql.session.DefaultSessionFactory;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import com.datastax.oss.driver.api.core.CqlSession;

/**
 * Factory for configuring a {@link CqlTemplate}.
 *
 * @author Matthew T. Adams
 * @author Mark Paluch
 */
public class CassandraCqlTemplateFactoryBean implements FactoryBean<CqlTemplate>, InitializingBean {

	private @Nullable CqlTemplate template;

	private @Nullable SessionFactory sessionFactory;

	/* (non-Javadoc)
	 * @see org.springframework.beans.factory.FactoryBean#getObject()
	 */
	@Override
	public CqlTemplate getObject() {
		return template;
	}

	/* (non-Javadoc)
	 * @see org.springframework.beans.factory.FactoryBean#getObjectType()
	 */
	@Override
	public Class<CqlTemplate> getObjectType() {
		return CqlTemplate.class;
	}

	/* (non-Javadoc)
	 * @see org.springframework.beans.factory.FactoryBean#isSingleton()
	 */
	@Override
	public boolean isSingleton() {
		return true;
	}

	/* (non-Javadoc)
	 * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 */
	@Override
	public void afterPropertiesSet() throws Exception {

		Assert.notNull(sessionFactory, "SessionFactory must not be null");

		this.template = new CqlTemplate(sessionFactory);
	}

	/**
	 * Sets the Cassandra {@link CqlSession} to use. The {@link CqlTemplate} will use the logged keyspace of the
	 * underlying {@link CqlSession}. Don't change the keyspace using CQL but use multiple {@link CqlSession} and
	 * {@link CqlTemplate} beans.
	 *
	 * @param session must not be {@literal null}.
	 */
	public void setSession(CqlSession session) {

		Assert.notNull(session, "Session must not be null");

		setSessionFactory(new DefaultSessionFactory(session));
	}

	/**
	 * Sets the Cassandra {@link SessionFactory} to use. The {@link CqlTemplate} will use the logged keyspace of the
	 * underlying {@link SessionFactory}. Don't change the keyspace using CQL but use an appropriate
	 * {@link SessionFactory}.
	 *
	 * @param sessionFactory must not be {@literal null}.
	 * @see SessionFactory
	 * @see org.springframework.data.cassandra.core.cql.session.lookup.AbstractRoutingSessionFactory
	 */
	public void setSessionFactory(SessionFactory sessionFactory) {

		Assert.notNull(sessionFactory, "SessionFactory must not be null");

		this.sessionFactory = sessionFactory;
	}
}
