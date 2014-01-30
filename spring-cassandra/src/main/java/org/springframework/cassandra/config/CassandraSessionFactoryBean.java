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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.cassandra.core.CqlTemplate;
import org.springframework.cassandra.support.CassandraExceptionTranslator;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

/**
 * Factory for configuring a Cassandra {@link Session}, which is a thread-safe singleton. As such, it is sufficient to
 * have one {@link Session} per application and keyspace.
 * 
 * @author Alex Shvid
 * @author Matthew T. Adams
 */

public class CassandraSessionFactoryBean implements FactoryBean<Session>, InitializingBean, DisposableBean,
		PersistenceExceptionTranslator {

	private static final Logger log = LoggerFactory.getLogger(CassandraSessionFactoryBean.class);

	protected Cluster cluster;
	protected Session session;
	protected String keyspaceName;
	protected List<String> startupScripts = new ArrayList<String>();
	protected List<String> shutdownScripts = new ArrayList<String>();
	protected final PersistenceExceptionTranslator exceptionTranslator = new CassandraExceptionTranslator();

	@Override
	public Session getObject() {
		return session;
	}

	@Override
	public Class<? extends Session> getObjectType() {
		return Session.class;
	}

	@Override
	public boolean isSingleton() {
		return true;
	}

	@Override
	public DataAccessException translateExceptionIfPossible(RuntimeException ex) {
		return exceptionTranslator.translateExceptionIfPossible(ex);
	}

	@Override
	public void afterPropertiesSet() throws Exception {

		Assert.notNull(cluster);

		session = StringUtils.hasText(keyspaceName) ? cluster.connect(keyspaceName) : cluster.connect();
		executeScripts(startupScripts);
	}

	/**
	 * Executes given scripts. Session must be connected when this method is called.
	 */
	protected void executeScripts(List<String> scripts) {

		if (scripts == null || scripts.size() == 0) {
			return;
		}

		CqlTemplate template = new CqlTemplate(session);

		for (String script : scripts) {

			if (log.isInfoEnabled()) {
				log.info("executing raw CQL [{}]", script);
			}

			template.execute(script);
		}
	}

	@Override
	public void destroy() throws Exception {

		executeScripts(shutdownScripts);
		session.shutdown();
	}

	/**
	 * Sets the keyspace name to connect to. Using <code>null</code>, empty string, or only whitespace will cause the
	 * system keyspace to be used.
	 */
	public void setKeyspaceName(String keyspaceName) {
		this.keyspaceName = keyspaceName;
	}

	/**
	 * Sets the cluster to use. Must not be null.
	 */
	public void setCluster(Cluster cluster) {
		if (cluster == null) {
			throw new IllegalArgumentException("cluster must not be null");
		}
		this.cluster = cluster;
	}

	/**
	 * Sets CQL scripts to be executed immediately after the session is connected.
	 */
	public void setStartupScripts(List<String> scripts) {
		this.startupScripts = scripts == null ? new ArrayList<String>() : new ArrayList<String>(scripts);
	}

	/**
	 * Returns an unmodifiable list of startup scripts.
	 */
	public List<String> getStartupScripts() {
		return Collections.unmodifiableList(startupScripts);
	}

	/**
	 * Sets CQL scripts to be executed immediately before the session is shutdown.
	 */
	public void setShutdownScripts(List<String> scripts) {
		this.shutdownScripts = scripts == null ? new ArrayList<String>() : new ArrayList<String>(scripts);
	}

	/**
	 * Returns an unmodifiable list of shutdown scripts.
	 */
	public List<String> getShutdownScripts() {
		return Collections.unmodifiableList(shutdownScripts);
	}
}
