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

import java.util.List;

import lombok.Data;
import lombok.extern.log4j.Log4j;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.support.PersistenceExceptionTranslator;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.ConstantReconnectionPolicy;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;

/**
 * Manages Cassandra Connection Factory returning Session for CQL Operations.
 * 
 * @author David Webb
 */
@Log4j
@Data 
public class CassandraConnectionFactoryBean implements FactoryBean<Session>, InitializingBean, DisposableBean,
	PersistenceExceptionTranslator {
	
	/*
	 * Connection Pool parameters with defaults.
	 * 
	 * All of these should be set when the Factory is initialized by the Spring Configuration method of choice.
	 */
	private List<String> seeds;
	private Integer port = 9042;
	private String localDataCenter;
	private int hostsPerRemoteDC;
	
	private Cluster cluster;
	private Session session;
	private CassandraExceptionTranslator exceptionTranslator = new CassandraExceptionTranslator();
	
	/**
	 * Default Constructor
	 */
	public CassandraConnectionFactoryBean() {
				
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.springframework.beans.factory.FactoryBean#getObjectType()
	 */
	public Class<? extends Session> getObjectType() {
		return Session.class;
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
	public Session getObject() throws Exception {

		return this.session;

	}

	/* (non-Javadoc)
	 * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 */
	public void afterPropertiesSet() throws Exception {

		Builder builder = Cluster.builder();
		builder.withPort(port);

		/*
		 * Add Contact Points
		 */
		for (String node : seeds) {
			builder.addContactPoint(node);
		}

		/*
		 * Set Load Balancing Policy to only write read from local data center
		 */
		if (localDataCenter != null) {
			builder.withLoadBalancingPolicy(new DCAwareRoundRobinPolicy(localDataCenter, hostsPerRemoteDC));
		}
		
		/*
		 * Set reconnection policy for dealing with down nodes.
		 */
		builder.withReconnectionPolicy(new ConstantReconnectionPolicy(30000));
		
		/*
		 * Use the default retry policy
		 */
		builder.withRetryPolicy(DefaultRetryPolicy.INSTANCE);
		
		cluster = builder.build();
		
		session = cluster.connect();
		
		Metadata metadata = cluster.getMetadata();
		log.info("Connected to cluster: " + metadata.getClusterName() + "; Dedicated DC: " + localDataCenter);
		for (Host host : metadata.getAllHosts()) {
			log.debug("Datacenter: " + host.getDatacenter() + " " +
						"; Host: " + host.getAddress() + " " +
						"; Rack: " + host.getRack());
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
		this.session.shutdown();
		this.cluster.shutdown();
		this.session = null;
		this.cluster = null;
	}
}
