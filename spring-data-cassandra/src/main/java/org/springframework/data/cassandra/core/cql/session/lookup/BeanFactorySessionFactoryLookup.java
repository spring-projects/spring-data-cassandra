/*
 * Copyright 2017-2021 the original author or authors.
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
package org.springframework.data.cassandra.core.cql.session.lookup;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.data.cassandra.SessionFactory;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * {@link SessionFactoryLookup} implementation based on a Spring {@link BeanFactory}.
 * <p>
 * Will lookup Spring managed beans identified by bean name, expecting them to be of type {@link SessionFactory}.
 *
 * @author Mark Paluch
 * @since 2.0
 * @see org.springframework.beans.factory.BeanFactory
 */
public class BeanFactorySessionFactoryLookup implements SessionFactoryLookup, BeanFactoryAware {

	private @Nullable BeanFactory beanFactory;

	/**
	 * Create a new instance of {@link BeanFactorySessionFactoryLookup}.
	 * <p>
	 * The BeanFactory to access must be set via {@link #setBeanFactory(BeanFactory)}.
	 *
	 * @see #setBeanFactory(BeanFactory)
	 */
	public BeanFactorySessionFactoryLookup() {}

	/**
	 * Create a new instance of {@link BeanFactorySessionFactoryLookup} given {@link BeanFactory}.
	 * <p>
	 * Use of this constructor is redundant if this object is being created by a Spring IoC container, as the supplied
	 * {@link BeanFactory} will be replaced by the {@link BeanFactory} that creates it ({@link BeanFactoryAware}
	 * contract). So only use this constructor if you are using this class outside the context of a Spring IoC container.
	 *
	 * @param beanFactory the bean factory to be used to lookup {@link SessionFactory session factories}, must not be
	 *          {@literal null}.
	 */
	public BeanFactorySessionFactoryLookup(BeanFactory beanFactory) {

		Assert.notNull(beanFactory, "BeanFactory must not be null");

		this.beanFactory = beanFactory;
	}

	/* (non-Javadoc)
	 * @see org.springframework.beans.factory.BeanFactoryAware#setBeanFactory(org.springframework.beans.factory.BeanFactory)
	 */
	@Override
	public void setBeanFactory(BeanFactory beanFactory) {
		this.beanFactory = beanFactory;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.session.lookup.SessionFactoryLookup#getSessionFactory(java.lang.String)
	 */
	@Override
	public SessionFactory getSessionFactory(String sessionFactoryName) throws SessionFactoryLookupFailureException {

		Assert.notNull(this.beanFactory, "BeanFactory must not be null");

		try {
			return this.beanFactory.getBean(sessionFactoryName, SessionFactory.class);
		} catch (BeansException ex) {
			throw new SessionFactoryLookupFailureException(
					String.format("Failed to look up SessionFactory bean with name [%s]", sessionFactoryName), ex);
		}
	}
}
