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

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.FactoryBean;

/**
 * Given List of Lists where all child Lists contain the same class, then a single level List of <T> is generated.
 * 
 * @author David Webb
 * @param <T>
 */
public class MultiLevelListFlattenerFactoryBean<T> implements FactoryBean<List<T>> {

	private List<List<T>> multiLevelList;

	@Override
	public List<T> getObject() throws Exception {
		List<T> list = new ArrayList<T>();

		for (List<T> topList : multiLevelList) {
			for (T t : topList) {
				list.add(t);
			}
		}

		return list;
	}

	@Override
	public Class<?> getObjectType() {
		return List.class;
	}

	@Override
	public boolean isSingleton() {
		return true;
	}

	/**
	 * @return Returns the multiLevelList.
	 */
	public List<List<T>> getMultiLevelList() {
		return multiLevelList;
	}

	/**
	 * @param multiLevelList The multiLevelList to set.
	 */
	public void setMultiLevelList(List<List<T>> multiLevelList) {
		this.multiLevelList = multiLevelList;
	}

}
