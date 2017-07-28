/*
 * Copyright 2013-2017 the original author or authors.
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
package org.springframework.data.cassandra.config;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.data.cassandra.core.cql.keyspace.CreateKeyspaceSpecification;
import org.springframework.data.cassandra.core.cql.keyspace.DefaultOption;
import org.springframework.data.cassandra.core.cql.keyspace.DropKeyspaceSpecification;
import org.springframework.data.cassandra.core.cql.keyspace.KeyspaceActionSpecification;
import org.springframework.data.cassandra.core.cql.keyspace.KeyspaceOption;
import org.springframework.data.cassandra.core.cql.keyspace.KeyspaceOption.ReplicationStrategy;
import org.springframework.data.cassandra.core.cql.keyspace.Option;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * A single keyspace XML Element can result in multiple actions. Example: {@literal CREATE_DROP}. This FactoryBean
 * inspects the action required to satisfy the keyspace element, and then returns a Set of atomic
 * {@link KeyspaceActionSpecification} required to satisfy the configuration action.
 *
 * @author David Webb
 */
public class KeyspaceActionSpecificationFactoryBean
		implements FactoryBean<Set<KeyspaceActionSpecification>>, InitializingBean, DisposableBean {

	private @Nullable KeyspaceAction action;

	private @Nullable String name;

	private List<String> networkTopologyDataCenters = new LinkedList<>();

	private List<String> networkTopologyReplicationFactors = new LinkedList<>();
	private @Nullable ReplicationStrategy replicationStrategy;

	private long replicationFactor;

	private boolean durableWrites = false;

	private boolean ifNotExists = false;

	private Set<KeyspaceActionSpecification> specs = new HashSet<>();

	/* (non-Javadoc)
	 * @see org.springframework.beans.factory.DisposableBean#destroy()
	 */
	@Override
	public void destroy() {
		action = null;
		name = null;
		networkTopologyDataCenters = new LinkedList<>();
		networkTopologyReplicationFactors = new LinkedList<>();
		replicationStrategy = null;
		specs = new HashSet<>();
	}

	/* (non-Javadoc)
	 * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 */
	@Override
	public void afterPropertiesSet() {

		Assert.hasText(name, "Keyspace Name is required for a Keyspace Action");
		Assert.notNull(action, "Keyspace Action is required for a Keyspace Action");

		switch (action) {
			case CREATE_DROP:
				specs.add(generateDropKeyspaceSpecification());
			case CREATE:
				// Assert.notNull(replicationStrategy, "Replication Strategy is required to create a Keyspace");
				specs.add(generateCreateKeyspaceSpecification());
				break;
			case ALTER:
				break;
		}
	}

	/**
	 * Generate a {@link CreateKeyspaceSpecification} for the keyspace.
	 *
	 * @return The {@link CreateKeyspaceSpecification}
	 */
	private CreateKeyspaceSpecification generateCreateKeyspaceSpecification() {

		CreateKeyspaceSpecification create = CreateKeyspaceSpecification.createKeyspace(name).ifNotExists(ifNotExists)
				.with(KeyspaceOption.DURABLE_WRITES, durableWrites);

		Map<Option, Object> replicationStrategyMap = new HashMap<>();
		replicationStrategyMap.put(new DefaultOption("class", String.class, true, false, true),
				replicationStrategy.getValue());

		if (replicationStrategy == ReplicationStrategy.SIMPLE_STRATEGY) {
			replicationStrategyMap.put(new DefaultOption("replication_factor", Long.class, true, false, false),
					replicationFactor);
		}

		if (replicationStrategy == ReplicationStrategy.NETWORK_TOPOLOGY_STRATEGY) {
			int i = 0;
			for (String datacenter : networkTopologyDataCenters) {
				replicationStrategyMap.put(new DefaultOption(datacenter, Long.class, true, false, false),
						networkTopologyReplicationFactors.get(i++));
			}
		}

		create.with(KeyspaceOption.REPLICATION, replicationStrategyMap);

		return create;
	}

	/**
	 * Generate a {@link DropKeyspaceSpecification} for the keyspace.
	 *
	 * @return The {@link DropKeyspaceSpecification}
	 */
	private DropKeyspaceSpecification generateDropKeyspaceSpecification() {
		return DropKeyspaceSpecification.dropKeyspace(getName());
	}

	/* (non-Javadoc)
	 * @see org.springframework.beans.factory.FactoryBean#getObject()
	 */
	@Override
	public Set<KeyspaceActionSpecification> getObject() {
		return specs;
	}

	/* (non-Javadoc)
	 * @see org.springframework.beans.factory.FactoryBean#getObjectType()
	 */
	@Override
	public Class<?> getObjectType() {
		return Set.class;
	}

	/* (non-Javadoc)
	 * @see org.springframework.beans.factory.FactoryBean#isSingleton()
	 */
	@Override
	public boolean isSingleton() {
		return false;
	}

	/**
	 * @return Returns the name.
	 */
	@Nullable
	public String getName() {
		return name;
	}

	/**
	 * @param name The name to set.
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * @return Returns the ifNotExists.
	 */
	public boolean isIfNotExists() {
		return ifNotExists;
	}

	/**
	 * @param ifNotExists The ifNotExists to set.
	 */
	public void setIfNotExists(boolean ifNotExists) {
		this.ifNotExists = ifNotExists;
	}

	/**
	 * @return Returns the action.
	 */
	@Nullable
	public KeyspaceAction getAction() {
		return action;
	}

	/**
	 * @param action The action to set.
	 */
	public void setAction(KeyspaceAction action) {
		this.action = action;
	}

	/**
	 * @return Returns the durableWrites.
	 */
	public boolean isDurableWrites() {
		return durableWrites;
	}

	/**
	 * @param durableWrites The durableWrites to set.
	 */
	public void setDurableWrites(boolean durableWrites) {
		this.durableWrites = durableWrites;
	}

	/**
	 * @return Returns the replicationStrategy.
	 */
	@Nullable
	public ReplicationStrategy getReplicationStrategy() {
		return replicationStrategy;
	}

	/**
	 * @param replicationStrategy The replicationStrategy to set.
	 */
	public void setReplicationStrategy(ReplicationStrategy replicationStrategy) {
		this.replicationStrategy = replicationStrategy;
	}

	/**
	 * @return Returns the networkTopologyDataCenters.
	 */
	public List<String> getNetworkTopologyDataCenters() {
		return networkTopologyDataCenters;
	}

	/**
	 * @param networkTopologyDataCenters The networkTopologyDataCenters to set.
	 */
	public void setNetworkTopologyDataCenters(List<String> networkTopologyDataCenters) {
		this.networkTopologyDataCenters = networkTopologyDataCenters;
	}

	/**
	 * @return Returns the networkTopologyReplicationFactors.
	 */
	public List<String> getNetworkTopologyReplicationFactors() {
		return networkTopologyReplicationFactors;
	}

	/**
	 * @param networkTopologyReplicationFactors The networkTopologyReplicationFactors to set.
	 */
	public void setNetworkTopologyReplicationFactors(List<String> networkTopologyReplicationFactors) {
		this.networkTopologyReplicationFactors = networkTopologyReplicationFactors;
	}

	/**
	 * @return Returns the replicationFactor.
	 */
	public long getReplicationFactor() {
		return replicationFactor;
	}

	/**
	 * @param replicationFactor The replicationFactor to set.
	 */
	public void setReplicationFactor(long replicationFactor) {
		this.replicationFactor = replicationFactor;
	}

}
