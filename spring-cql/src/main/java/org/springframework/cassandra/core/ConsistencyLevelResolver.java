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
package org.springframework.cassandra.core;

/**
 * Determine driver consistency level based on ConsistencyLevel
 * 
 * @author David Webb
 */
public final class ConsistencyLevelResolver {

	/**
	 * No instances allowed
	 */
	private ConsistencyLevelResolver() {}

	/**
	 * Decode the generic spring data cassandra enum to the type required by the DataStax Driver.
	 * 
	 * @param level Spring consistency level equivalent
	 * @return The DataStax Driver Consistency Level.
	 */
	public static com.datastax.driver.core.ConsistencyLevel resolve(ConsistencyLevel level) {

		com.datastax.driver.core.ConsistencyLevel resolvedLevel = com.datastax.driver.core.ConsistencyLevel.ONE;

		/*
		 * Determine the driver level based on our enum
		 */
		switch (level) {
			case ONE:
				resolvedLevel = com.datastax.driver.core.ConsistencyLevel.ONE;
				break;
			case LOCAL_ONE:
				resolvedLevel = com.datastax.driver.core.ConsistencyLevel.LOCAL_ONE;
				break;
			case ALL:
				resolvedLevel = com.datastax.driver.core.ConsistencyLevel.ALL;
				break;
			case ANY:
				resolvedLevel = com.datastax.driver.core.ConsistencyLevel.ANY;
				break;
			case EACH_QUORUM:
				resolvedLevel = com.datastax.driver.core.ConsistencyLevel.EACH_QUORUM;
				break;
			case LOCAL_QUORUM:
				resolvedLevel = com.datastax.driver.core.ConsistencyLevel.LOCAL_QUORUM;
				break;
			case QUORUM:
				resolvedLevel = com.datastax.driver.core.ConsistencyLevel.QUORUM;
				break;
			case THREE:
				resolvedLevel = com.datastax.driver.core.ConsistencyLevel.THREE;
				break;
			case TWO:
				resolvedLevel = com.datastax.driver.core.ConsistencyLevel.TWO;
				break;
			case SERIAL:
				resolvedLevel = com.datastax.driver.core.ConsistencyLevel.SERIAL;
				break;
			case LOCAL_SERIAL:
				resolvedLevel = com.datastax.driver.core.ConsistencyLevel.LOCAL_SERIAL;
				break;
			default:
				break;
		}

		return resolvedLevel;

	}

}
