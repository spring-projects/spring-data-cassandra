/*
 * Copyright 2019-2021 the original author or authors.
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
package org.springframework.data.cassandra.core.cql.session.init;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.data.cassandra.SessionFactory;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Used to {@linkplain #setKeyspacePopulator set up} a keyspace during initialization and {@link #setKeyspaceCleaner
 * clean up} a keyspace during destruction.
 *
 * @author Mark Paluch
 * @since 3.0
 * @see KeyspacePopulator
 */
public class SessionFactoryInitializer implements InitializingBean, DisposableBean {

	private @Nullable SessionFactory sessionFactory;

	private @Nullable KeyspacePopulator keyspacePopulator;

	private @Nullable KeyspacePopulator keyspaceCleaner;

	private boolean enabled = true;

	/**
	 * The {@link SessionFactory} for the keyspace to populate when this component is initialized and to clean up when
	 * this component is shut down.
	 * <p>
	 * This property is mandatory with no default provided.
	 *
	 * @param sessionFactory the SessionFactory.
	 */
	public void setSessionFactory(SessionFactory sessionFactory) {
		this.sessionFactory = sessionFactory;
	}

	/**
	 * Set the {@link KeyspacePopulator} to execute during the bean initialization phase.
	 *
	 * @param keyspacePopulator the {@link KeyspacePopulator} to use during initialization.
	 * @see #setKeyspaceCleaner
	 */
	public void setKeyspacePopulator(KeyspacePopulator keyspacePopulator) {
		this.keyspacePopulator = keyspacePopulator;
	}

	/**
	 * Set the {@link KeyspacePopulator} to execute during the bean destruction phase, cleaning up the keyspace and
	 * leaving it in a known state for others.
	 *
	 * @param keyspaceCleaner the {@link KeyspacePopulator} to use during destruction.
	 * @see #setKeyspacePopulator
	 */
	public void setKeyspaceCleaner(KeyspacePopulator keyspaceCleaner) {
		this.keyspaceCleaner = keyspaceCleaner;
	}

	/**
	 * Flag to explicitly enable or disable the {@linkplain #setKeyspacePopulator keyspace populator} and
	 * {@linkplain #setKeyspaceCleaner keyspace cleaner}.
	 *
	 * @param enabled {@literal true} if the keyspace populator and keyspace cleaner should be called on startup and
	 *          shutdown, respectively.
	 */
	public void setEnabled(boolean enabled) {
		this.enabled = enabled;
	}

	/**
	 * Use the {@linkplain #setKeyspacePopulator keyspace populator} to set up the keyspace.
	 */
	@Override
	public void afterPropertiesSet() {
		execute(this.keyspacePopulator);
	}

	/**
	 * Use the {@linkplain #setKeyspaceCleaner keyspace cleaner} to clean up the keyspace.
	 */
	@Override
	public void destroy() {
		execute(this.keyspaceCleaner);
	}

	private void execute(@Nullable KeyspacePopulator populator) {

		Assert.state(this.sessionFactory != null, "SessionFactory must be set");

		if (this.enabled && populator != null) {
			populator.populate(this.sessionFactory.getSession());
		}
	}
}
