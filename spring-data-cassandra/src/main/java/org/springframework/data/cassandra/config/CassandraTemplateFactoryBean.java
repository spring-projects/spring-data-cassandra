/*
 * Copyright 2013-2021 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
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
import org.springframework.data.cassandra.core.CassandraTemplate;
import org.springframework.data.cassandra.core.convert.CassandraConverter;
import org.springframework.data.cassandra.core.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.core.cql.CqlOperations;
import org.springframework.data.cassandra.core.cql.session.DefaultSessionFactory;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import com.datastax.oss.driver.api.core.CqlSession;

/**
 * Factory for configuring a {@link CassandraTemplate}.
 *
 * @author Matthew T. Adams
 * @author Mark Paluch
 */
public class CassandraTemplateFactoryBean implements FactoryBean<CassandraTemplate>, InitializingBean {

	protected @Nullable SessionFactory sessionFactory;

	protected @Nullable CqlOperations cqlOperations;

	protected @Nullable CassandraConverter converter;

	/*
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 */
	@Override
	public void afterPropertiesSet() throws Exception {

		if (cqlOperations == null && sessionFactory == null) {
			throw new IllegalArgumentException("Either Session/SessionFactory or CqlOperations must be set");
		}

		if (converter == null) {
			converter = new MappingCassandraConverter();
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.FactoryBean#getObject()
	 */
	@Override
	public CassandraTemplate getObject() throws Exception {

		if (cqlOperations != null) {
			return new CassandraTemplate(cqlOperations, converter);
		}

		return new CassandraTemplate(sessionFactory, converter);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.FactoryBean#getObjectType()
	 */
	@Override
	public Class<CassandraTemplate> getObjectType() {
		return CassandraTemplate.class;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.FactoryBean#isSingleton()
	 */
	@Override
	public boolean isSingleton() {
		return true;
	}

	/**
	 * Sets the Cassandra {@link CqlSession} to use. The {@link CassandraTemplate} will use the logged keyspace of the
	 * underlying {@link CqlSession}. Don't change the keyspace using CQL but use a {@link SessionFactory}.
	 *
	 * @param session must not be {@literal null}.
	 */
	public void setSession(CqlSession session) {

		Assert.notNull(session, "Session must not be null");

		setSessionFactory(new DefaultSessionFactory(session));
	}

	/**
	 * Sets the Cassandra {@link SessionFactory} to use. The {@link CassandraTemplate} will use the logged keyspace of the
	 * underlying {@link CqlSession}. Don't change the keyspace using CQL.
	 *
	 * @param sessionFactory must not be {@literal null}.
	 * @since 2.0
	 */
	public void setSessionFactory(SessionFactory sessionFactory) {

		Assert.notNull(sessionFactory, "SessionFactory must not be null");

		this.sessionFactory = sessionFactory;
	}

	/**
	 * Sets the Cassandra {@link CqlOperations} to use. The {@link CassandraTemplate} will use the logged keyspace of the
	 * underlying {@link CqlSession}. Don't change the keyspace using CQL but use
	 * {@link #setSessionFactory(SessionFactory)}.
	 *
	 * @param cqlOperations must not be {@literal null}.
	 * @since 2.0
	 */
	public void setCqlOperations(CqlOperations cqlOperations) {

		Assert.notNull(cqlOperations, "CqlOperations must not be null");

		this.cqlOperations = cqlOperations;
	}

	/**
	 * Set the {@link CassandraConverter} to use.
	 *
	 * @param converter must not be {@literal null}.
	 */
	public void setConverter(CassandraConverter converter) {

		Assert.notNull(converter, "Converter must not be null");

		this.converter = converter;
	}
}
