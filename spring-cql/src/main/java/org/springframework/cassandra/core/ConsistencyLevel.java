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

/**
 * Generic Consistency Levels associated with Cassandra.
 *
 * @author David Webb
 * @author Antoine Toulme
 * @deprecated as of 1.5, use the driver's {@link com.datastax.driver.core.ConsistencyLevel}.
 */
@Deprecated
public enum ConsistencyLevel {

	ANY, ONE, TWO, THREE,

	/**
	 * @deprecated as of 1.5, use {@link #QUORUM}
	 */
	@Deprecated QUOROM,

	/**
	 * @deprecated as of 1.5, use {@link #LOCAL_QUORUM}
	 */
	@Deprecated LOCAL_QUOROM,

	/**
	 * @deprecated as of 1.5, use {@link #EACH_QUORUM}
	 */
	@Deprecated EACH_QUOROM,

	ALL, LOCAL_ONE, SERIAL, LOCAL_SERIAL,

	/**
	 * @since 1.5
	 */
	QUORUM,

	/**
	 * @since 1.5
	 */
	LOCAL_QUORUM,

	/**
	 * @since 1.5
	 */
	EACH_QUORUM,
}
