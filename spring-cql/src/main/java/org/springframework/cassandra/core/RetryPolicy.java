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
 * Retry Policies associated with Cassandra. This enum type is a shortcut to the predefined
 * {@link com.datastax.driver.core.policies.RetryPolicy policies} that come with the driver.
 *
 * @author David Webb
 * @author Mark Paluch
 * @deprecated as of 1.5, use the driver's {@link com.datastax.driver.core.policies.RetryPolicy}.
 */
@Deprecated
public enum RetryPolicy {

	DEFAULT, DOWNGRADING_CONSISTENCY, FALLTHROUGH,

	/**
	 * The {@link com.datastax.driver.core.policies.LoggingRetryPolicy} is just a decorator for other policies so it can't
	 * be applied to {@link QueryOptions}/{@link WriteOptions}
	 */
	LOGGING

}
