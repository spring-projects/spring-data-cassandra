/*
 * Copyright 2010-2013 the original author or authors.
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
package org.springframework.data.cassandra.core;

import lombok.Data;
import lombok.extern.log4j.Log4j;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.support.PersistenceExceptionTranslator;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

/**
 * Manages Connection Pool for each Keyspace.
 * 
 * @author David Webb
 */
@Log4j
@Data 
public class KeyspaceFactoryBean implements FactoryBean<Keyspace>, InitializingBean, DisposableBean,
	PersistenceExceptionTranslator {
	
	/*
	 * Connection Pool parameters with defaults.
	 * 
	 * All of these should be set when the Factory is initialized by the Spring Configuration method of choice.
	 */
	private String host = "localhost";
	private Integer port = 9160;
	private String keyspaceName = "test_keyspace";
	private String poolName = "myConnectionPool";
	private NodeDiscoveryType nodeDiscoveryType = NodeDiscoveryType.NONE;
	private int socketTimeout = 30000;
	
	private Keyspace keyspace;
	private AstyanaxContext<Keyspace> context;
	private ThriftExceptionTranslator exceptionTranslator = new ThriftExceptionTranslator();
	

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.springframework.beans.factory.FactoryBean#getObjectType()
	 */
	public Class<? extends Keyspace> getObjectType() {
		return Keyspace.class;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.springframework.beans.factory.FactoryBean#isSingleton()
	 */
	public boolean isSingleton() {
		return true;
	}

	/* (non-Javadoc)
	 * @see org.springframework.beans.factory.FactoryBean#getObject()
	 */
	public Keyspace getObject() throws Exception {

		return this.context.getClient();

	}

	/* (non-Javadoc)
	 * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 */
	public void afterPropertiesSet() throws Exception {

		log.info("Host -> " + this.host);
		log.info("Port -> " + this.port);
		log.info("Keyspace -> " + this.keyspaceName);
		
		try {
			context = new AstyanaxContext.Builder()
					.forCluster("Test Cluster")
					.forKeyspace(this.keyspaceName)
					.withAstyanaxConfiguration(
							new AstyanaxConfigurationImpl()
									.setDiscoveryType(this.nodeDiscoveryType))
					.withConnectionPoolConfiguration(
							new ConnectionPoolConfigurationImpl(this.poolName)
								.setPort(this.port)
								.setSocketTimeout(this.socketTimeout)
								.setMaxTimeoutWhenExhausted(2000)
								.setMaxConnsPerHost(20)
								.setInitConnsPerHost(5)
								.setSeeds(this.host + ":" + this.port)
							)
					.withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
					.buildKeyspace(ThriftFamilyFactory.getInstance());

			context.start();
		} catch (Throwable e) {
			throw new IllegalStateException("Failed to prepare CassandraBolt",
					e);
		}
		
	}

	/* (non-Javadoc)
	 * @see org.springframework.dao.support.PersistenceExceptionTranslator#translateExceptionIfPossible(java.lang.RuntimeException)
	 */
	public DataAccessException translateExceptionIfPossible(RuntimeException ex) {
		return exceptionTranslator.translateExceptionIfPossible(ex);
	}

	/* (non-Javadoc)
	 * @see org.springframework.beans.factory.DisposableBean#destroy()
	 */
	public void destroy() throws Exception {
		this.context.getConnectionPool().shutdown();
		this.context = null;
		this.keyspace = null;
	}
}
