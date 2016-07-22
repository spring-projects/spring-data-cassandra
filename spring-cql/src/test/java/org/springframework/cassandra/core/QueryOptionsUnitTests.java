/*
 * Copyright 2016 the original author or authors.
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
package org.springframework.cassandra.core;

import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.FallthroughRetryPolicy;
import com.datastax.driver.core.policies.LoggingRetryPolicy;

/**
 * Unit tests for {@link QueryOptions}.
 * 
 * @author Mark Paluch
 */
public class QueryOptionsUnitTests {

	/**
	 * @see DATACASS-202
	 */
	@Test
	public void buildQueryOptions() {

		QueryOptions queryOptions = QueryOptions.builder() //
				.consistencyLevel(ConsistencyLevel.ANY) //
				.retryPolicy(RetryPolicy.DEFAULT) //
				.readTimeout(1, TimeUnit.SECONDS)//
				.fetchSize(10)//
				.tracing(true)//
				.build(); //

		assertThat((Class) queryOptions.getClass(), is(equalTo((Class) QueryOptions.class)));
		assertThat(queryOptions.getRetryPolicy(), is(RetryPolicy.DEFAULT));
		assertThat(queryOptions.getConsistencyLevel(), is(nullValue()));
		assertThat(queryOptions.getDriverConsistencyLevel(), is(ConsistencyLevel.ANY));
		assertThat(queryOptions.getReadTimeout(), is(1000L));
		assertThat(queryOptions.getFetchSize(), is(10));
		assertThat(queryOptions.getTracing(), is(true));
	}

	/**
	 * @see DATACASS-202
	 */
	@Test
	public void buildQueryOptionsWithDriverRetryPolicy() {

		QueryOptions writeOptions = QueryOptions.builder() //
				.retryPolicy(new LoggingRetryPolicy(DefaultRetryPolicy.INSTANCE)) //
				.build(); //

		assertThat(writeOptions.getRetryPolicy(), is(nullValue()));
		assertThat(writeOptions.getDriverRetryPolicy(), is(instanceOf(LoggingRetryPolicy.class)));
	}

	/**
	 * @see DATACASS-202
	 */
	@Test
	public void buildQueryOptionsWithRetryPolicy() {

		QueryOptions writeOptions = QueryOptions.builder() //
				.retryPolicy(RetryPolicy.DOWNGRADING_CONSISTENCY) //
				.build(); //

		assertThat(writeOptions.getRetryPolicy(), is(RetryPolicy.DOWNGRADING_CONSISTENCY));
		assertThat(writeOptions.getDriverRetryPolicy(), is(nullValue()));
	}

	/**
	 * @see DATACASS-202
	 */
	@Test(expected = IllegalStateException.class)
	public void builderShouldRejectSettingOurAndDriverRetryPolicy() {

		QueryOptions.builder() //
				.retryPolicy(RetryPolicy.DEFAULT).retryPolicy(FallthroughRetryPolicy.INSTANCE);
	}

	/**
	 * @see DATACASS-202
	 */
	@Test(expected = IllegalStateException.class)
	public void builderShouldRejectSettingDriverAndOurRetryPolicy() {

		QueryOptions.builder() //
				.retryPolicy(FallthroughRetryPolicy.INSTANCE)//
				.retryPolicy(RetryPolicy.DEFAULT);

	}

	/**
	 * @see DATACASS-202
	 */
	@Test(expected = IllegalStateException.class)
	public void shouldRejectSettingOurAndDriverRetryPolicy() {

		QueryOptions queryOptions = new QueryOptions();
		queryOptions.setRetryPolicy(RetryPolicy.DEFAULT);
		queryOptions.setRetryPolicy(FallthroughRetryPolicy.INSTANCE);
	}

	/**
	 * @see DATACASS-202
	 */
	@Test(expected = IllegalStateException.class)
	public void shouldRejectSettingDriverAndOurRetryPolicy() {

		QueryOptions queryOptions = new QueryOptions();
		queryOptions.setRetryPolicy(FallthroughRetryPolicy.INSTANCE);
		queryOptions.setRetryPolicy(RetryPolicy.DEFAULT);
	}

	/**
	 * @see DATACASS-202
	 */
	@Test(expected = IllegalStateException.class)
	public void shouldRejectSettingOurAndDriverConsistencyLevel() {

		QueryOptions queryOptions = new QueryOptions();
		queryOptions.setConsistencyLevel(org.springframework.cassandra.core.ConsistencyLevel.ANY);
		queryOptions.setConsistencyLevel(ConsistencyLevel.ANY);
	}

	/**
	 * @see DATACASS-202
	 */
	@Test(expected = IllegalStateException.class)
	public void shouldRejectSettingDriverAndOurConsistencyLevel() {

		QueryOptions queryOptions = new QueryOptions();
		queryOptions.setConsistencyLevel(ConsistencyLevel.ANY);
		queryOptions.setConsistencyLevel(org.springframework.cassandra.core.ConsistencyLevel.ANY);
	}
}
