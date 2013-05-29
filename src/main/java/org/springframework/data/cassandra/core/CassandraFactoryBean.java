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

import lombok.extern.log4j.Log4j;

import org.springframework.beans.factory.InitializingBean;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

/**
 * Convenient factory for configuring Cassandra Keyspace.
 * 
 * @author Brian O'Neill
 * @author David Webb
 */
@Log4j
public class CassandraFactoryBean implements InitializingBean {
	
	private String host = "localhost";
	private Integer port = 9160;
	private String keyspaceName;
	
	protected Keyspace keyspace;
	protected AstyanaxContext<Keyspace> context;

	public void setHost(String host) {
		this.host = host;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public void setKeyspace(String keyspace) {
		this.keyspaceName = keyspace;
	}

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

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
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
									.setDiscoveryType(NodeDiscoveryType.NONE))
					.withConnectionPoolConfiguration(
							new ConnectionPoolConfigurationImpl("MyConnectionPool")
								.setPort(this.port)
								.setSocketTimeout(30000)
								.setMaxTimeoutWhenExhausted(2000)
								.setMaxConnsPerHost(20)
								.setInitConnsPerHost(5)
								.setSeeds(this.host + ":" + this.port)
							)
					.withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
					//.withConnectionPoolMonitor(new Slf4jConnectionPoolMonitorImpl())
					
					.buildKeyspace(ThriftFamilyFactory.getInstance());

			context.start();
			this.keyspace = context.getClient();
		} catch (Throwable e) {
			throw new IllegalStateException("Failed to prepare CassandraBolt",
					e);
		}
	}
	
	/**
	 * Return the Client Connection for interfacing with Cassandra
	 * 
	 * @return
	 */
	public Keyspace getClient() {
		
		try {
			this.keyspace.describeKeyspace();
		} catch (ConnectionException e) {
			log.warn("Connection to Cassandra is not available...reinitializing pool.");
			try {
				afterPropertiesSet();
			} catch (Exception e1) {
				e1.printStackTrace();
			}
		}
		
		return this.keyspace;
	}
}
