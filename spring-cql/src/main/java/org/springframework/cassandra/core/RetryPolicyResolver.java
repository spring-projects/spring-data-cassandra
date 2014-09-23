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

import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy;
import com.datastax.driver.core.policies.FallthroughRetryPolicy;

/**
 * Determine driver query retry policy
 * 
 * @author David Webb
 */
public final class RetryPolicyResolver {

	/**
	 * No instances allowed
	 */
	private RetryPolicyResolver() {}

	/**
	 * Decode the generic spring data cassandra enum to the type required by the DataStax Driver.
	 * 
	 * @param level
	 * @return The DataStax Driver Consistency Level.
	 */
	public static com.datastax.driver.core.policies.RetryPolicy resolve(RetryPolicy policy) {

		com.datastax.driver.core.policies.RetryPolicy resolvedPolicy = DefaultRetryPolicy.INSTANCE;

		/*
		 * Determine the driver level based on our enum
		 */
		switch (policy) {
			case DEFAULT:
				resolvedPolicy = DefaultRetryPolicy.INSTANCE;
				break;
			case DOWNGRADING_CONSISTENCY:
				resolvedPolicy = DowngradingConsistencyRetryPolicy.INSTANCE;
				break;
			case FALLTHROUGH:
				resolvedPolicy = FallthroughRetryPolicy.INSTANCE;
				break;
			default:
				resolvedPolicy = DefaultRetryPolicy.INSTANCE;
				break;
		}

		return resolvedPolicy;

	}
}
