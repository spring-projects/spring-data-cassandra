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
 * Contains Query Options for Cassandra queries. This controls the Consistency Tuning and Retry Policy for a Query.
 * 
 * @author David Webb
 */
public class QueryOptions {

	private ConsistencyLevel consistencyLevel;
	private RetryPolicy retryPolicy;

	public QueryOptions() {}

	public QueryOptions(ConsistencyLevel consistencyLevel, RetryPolicy retryPolicy) {
		setConsistencyLevel(consistencyLevel);
		setRetryPolicy(retryPolicy);
	}

	/**
	 * @return Returns the consistencyLevel.
	 */
	public ConsistencyLevel getConsistencyLevel() {
		return consistencyLevel;
	}

	/**
	 * @param consistencyLevel The consistencyLevel to set.
	 */
	public void setConsistencyLevel(ConsistencyLevel consistencyLevel) {
		this.consistencyLevel = consistencyLevel;
	}

	/**
	 * @return Returns the retryPolicy.
	 */
	public RetryPolicy getRetryPolicy() {
		return retryPolicy;
	}

	/**
	 * @param retryPolicy The retryPolicy to set.
	 */
	public void setRetryPolicy(RetryPolicy retryPolicy) {
		this.retryPolicy = retryPolicy;
	}
}
