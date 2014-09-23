/*
 * Copyright 2013-2014 the original author or authors.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.cassandra.config;

import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.FactoryBean;

/**
 * Given Set of Sets where all child Sets contain the same class, then a single level Set of <T> is generated.
 * 
 * @author David Webb
 * @param <T>
 */
public class MultiLevelSetFlattenerFactoryBean<T> implements FactoryBean<Set<T>> {

	private final static Logger log = LoggerFactory.getLogger(MultiLevelSetFlattenerFactoryBean.class);

	private Set<Set<T>> multiLevelSet;

	@Override
	public Set<T> getObject() throws Exception {
		Set<T> set = new HashSet<T>();

		for (Set<T> topSet : multiLevelSet) {
			for (T t : topSet) {
				log.debug(t.toString());
				log.debug("Set contains -> " + set.contains(t));
				set.add(t);
			}
		}

		return set;
	}

	@Override
	public Class<?> getObjectType() {
		return Set.class;
	}

	@Override
	public boolean isSingleton() {
		return true;
	}

	/**
	 * @return Returns the multiLevelSet.
	 */
	public Set<Set<T>> getMultiLevelSet() {
		return multiLevelSet;
	}

	/**
	 * @param multiLevelSet The multiLevelSet to set.
	 */
	public void setMultiLevelSet(Set<Set<T>> multiLevelSet) {
		this.multiLevelSet = multiLevelSet;
	}

}
