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
package org.springframework.data.cassandra.config;

import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.data.cassandra.config.KeyspaceActionSpecificationFactory.KeyspaceActionSpecificationFactoryBuilder;
import org.springframework.data.cassandra.core.cql.keyspace.DataCenterReplication;
import org.springframework.data.cassandra.core.cql.keyspace.KeyspaceActionSpecification;
import org.springframework.data.cassandra.core.cql.keyspace.KeyspaceOption.ReplicationStrategy;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * A single keyspace XML Element can result in multiple actions. Example: {@literal CREATE_DROP}. This FactoryBean
 * inspects the action required to satisfy the keyspace element, and then returns a Set of atomic
 * {@link KeyspaceActionSpecification} required to satisfy the configuration action.
 *
 * @author David Webb
 * @author Mark Paluch
 */
@SuppressWarnings({ "unused", "WeakerAccess" })
public class KeyspaceActionSpecificationFactoryBean implements FactoryBean<KeyspaceActions>, InitializingBean {

	private KeyspaceAction action = KeyspaceAction.NONE;

	private @Nullable String name;

	private List<String> networkTopologyDataCenters = new LinkedList<>();

	private List<String> networkTopologyReplicationFactors = new LinkedList<>();

	private ReplicationStrategy replicationStrategy = ReplicationStrategy.SIMPLE_STRATEGY;

	private int replicationFactor;

	private boolean durableWrites = false;

	private boolean ifNotExists = false;

	private @Nullable KeyspaceActions actions;

	/* (non-Javadoc)
	 * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 */
	@Override
	public void afterPropertiesSet() {

		Assert.hasText(name, "Keyspace name is required for a keyspace action");
		Assert.notNull(action, "Keyspace action is required for a keyspace action");

		KeyspaceActionSpecificationFactoryBuilder builder = KeyspaceActionSpecificationFactory.builder(name)
				.durableWrites(durableWrites);

		if (replicationStrategy == ReplicationStrategy.SIMPLE_STRATEGY) {
			builder.simpleReplication(replicationFactor);
		}

		if (replicationStrategy == ReplicationStrategy.NETWORK_TOPOLOGY_STRATEGY) {

			int i = 0;
			for (String datacenter : networkTopologyDataCenters) {
				builder.withDataCenter(
						DataCenterReplication.of(datacenter, Integer.parseInt(networkTopologyReplicationFactors.get(i++))));
			}
		}

		KeyspaceActionSpecificationFactory factory = builder.build();

		this.actions = createActions(factory);
	}

	private KeyspaceActions createActions(KeyspaceActionSpecificationFactory factory) {

		switch (action) {
			case NONE:
				return new KeyspaceActions();
			case CREATE_DROP:
				return new KeyspaceActions(factory.create(ifNotExists), factory.drop(ifNotExists));
			case CREATE:
				return new KeyspaceActions(factory.create(ifNotExists));
			case ALTER:
				return new KeyspaceActions(factory.alter());
			default:
				throw new IllegalStateException(String.format("KeyspaceAction %s not supported", action));
		}
	}

	/* (non-Javadoc)
	 * @see org.springframework.beans.factory.FactoryBean#getObject()
	 */
	@Override
	public KeyspaceActions getObject() {
		return actions;
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
	public void setReplicationFactor(int replicationFactor) {
		this.replicationFactor = replicationFactor;
	}
}
