/*
 * Copyright 2013-2016 the original author or authors.
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

import org.springframework.util.Assert;

/**
 * Determine driver consistency level based on ConsistencyLevel
 *
 * @author David Webb
 * @author Antoine Toulme
 * @deprecated as of 1.5, use the driver's {@link com.datastax.driver.core.ConsistencyLevel}.
 */
@Deprecated
public final class ConsistencyLevelResolver {

	/**
	 * No instances allowed
	 */
	private ConsistencyLevelResolver() {}

	/**
	 * Decode the generic spring data cassandra enum to the type required by the DataStax Driver.
	 *
	 * @param level the consistency level to resolve, must not be {@literal null}.
	 * @return The DataStax Driver Consistency Level.
	 */
	@SuppressWarnings("deprecation")
	public static com.datastax.driver.core.ConsistencyLevel resolve(ConsistencyLevel level) {

		Assert.notNull(level, "ConsistencyLevel must not be null");

		// Determine the driver ConsistencyLevel based on SD Cassandra's enum
		switch (level) {
			case ONE:
				return com.datastax.driver.core.ConsistencyLevel.ONE;
			case LOCAL_ONE:
				return com.datastax.driver.core.ConsistencyLevel.LOCAL_ONE;
			case ALL:
				return com.datastax.driver.core.ConsistencyLevel.ALL;
			case ANY:
				return com.datastax.driver.core.ConsistencyLevel.ANY;
			case EACH_QUORUM:
			case EACH_QUOROM:
				return com.datastax.driver.core.ConsistencyLevel.EACH_QUORUM;
			case LOCAL_QUORUM:
			case LOCAL_QUOROM:
				return com.datastax.driver.core.ConsistencyLevel.LOCAL_QUORUM;
			case QUORUM:
			case QUOROM:
				return com.datastax.driver.core.ConsistencyLevel.QUORUM;
			case THREE:
				return com.datastax.driver.core.ConsistencyLevel.THREE;
			case TWO:
				return com.datastax.driver.core.ConsistencyLevel.TWO;
			case SERIAL:
				return com.datastax.driver.core.ConsistencyLevel.SERIAL;
			case LOCAL_SERIAL:
				return com.datastax.driver.core.ConsistencyLevel.LOCAL_SERIAL;
			default:
				throw new IllegalArgumentException(String.format("ConsistencyLevel [%s] not supported", level));
		}
	}
}
