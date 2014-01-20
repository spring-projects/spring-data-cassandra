/*
 * Copyright 2011-2014 the original author or authors.
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

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.cassandra.core.keyspace.CreateKeyspaceSpecification;
import org.springframework.cassandra.core.keyspace.DefaultOption;
import org.springframework.cassandra.core.keyspace.DropKeyspaceSpecification;
import org.springframework.cassandra.core.keyspace.KeyspaceActionSpecification;
import org.springframework.cassandra.core.keyspace.KeyspaceOption;
import org.springframework.cassandra.core.keyspace.Option;
import org.springframework.util.Assert;

/**
 * @author David Webb (dwebb@brightmove.com)
 * 
 */
public class KeyspaceActionSpecificationFactoryBean implements FactoryBean<Set<KeyspaceActionSpecification<?>>>,
		InitializingBean, DisposableBean {

	private final static Logger log = LoggerFactory.getLogger(KeyspaceActionSpecificationFactoryBean.class);

	private KeyspaceAction action;
	private String name;
	private Map<Option, Object> replicationOptions = new LinkedHashMap<Option, Object>();
	private boolean durableWrites = false;
	private boolean ifNotExists = false;

	private Set<KeyspaceActionSpecification<?>> specs = new HashSet<KeyspaceActionSpecification<?>>();

	@Override
	public void destroy() throws Exception {
		name = null;
		replicationOptions = null;
		specs = null;
	}

	@Override
	public void afterPropertiesSet() throws Exception {

		Assert.hasText(name, "Keyspace Name is required for a Keyspace Action");
		Assert.notNull(action, "Keyspace Action is required for a Keyspace Action");

		switch (action) {
		case CREATE_DROP:
			specs.add(generateDropKeyspaceSpecification());
		case CREATE:
			specs.add(generateCreateKeyspaceSpecification());
			break;
		case ALTER:
			break;
		}

	}

	private CreateKeyspaceSpecification generateCreateKeyspaceSpecification() {
		CreateKeyspaceSpecification create = new CreateKeyspaceSpecification();
		create.name(name).ifNotExists(ifNotExists).with(KeyspaceOption.DURABLE_WRITES, durableWrites);
		if (replicationOptions != null && replicationOptions.size() > 0) {
			create.with(KeyspaceOption.REPLICATION, replicationOptions);
		} else {
			Map<Option, Object> defaultReplicationStrategyMap = new HashMap<Option, Object>();
			defaultReplicationStrategyMap.put(new DefaultOption("class", String.class, true, false, true),
					KeyspaceOption.ReplicationStrategy.SIMPLE_STRATEGY);
			defaultReplicationStrategyMap.put(new DefaultOption("replication_factor", String.class, true, false, false), "1");
			create.with(KeyspaceOption.REPLICATION, defaultReplicationStrategyMap);
		}
		return create;
	}

	private DropKeyspaceSpecification generateDropKeyspaceSpecification() {
		DropKeyspaceSpecification drop = new DropKeyspaceSpecification();
		drop.name(getName());
		return drop;
	}

	@Override
	public Set<KeyspaceActionSpecification<?>> getObject() throws Exception {
		return specs;
	}

	@Override
	public Class<?> getObjectType() {
		return Set.class;
	}

	@Override
	public boolean isSingleton() {
		return false;
	}

	/**
	 * @return Returns the name.
	 */
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
	 * @return Returns the replicationOptions.
	 */
	public Map<Option, Object> getReplicationOptions() {
		return replicationOptions;
	}

	/**
	 * @param replicationOptions The replicationOptions to set.
	 */
	public void setReplicationOptions(Map<Option, Object> replicationOptions) {
		this.replicationOptions = replicationOptions;
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

}
