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

import com.datastax.driver.core.policies.FallthroughRetryPolicy;

/**
 * Unit tests for {@link WriteOptions}.
 * 
 * @author Mark Paluch
 */
public class WriteOptionsUnitTests {

	/**
	 * @see DATACASS-202
	 */
	@Test
	public void buildWriteOptions() {

		WriteOptions writeOptions = WriteOptions.builder() //
				.consistencyLevel(com.datastax.driver.core.ConsistencyLevel.ANY) //
				.ttl(123) //
				.retryPolicy(RetryPolicy.DEFAULT) //
				.readTimeout(1)//
				.fetchSize(10)//
				.withTracing()//
				.build(); //

		assertThat(writeOptions.getTtl(), is(123));
		assertThat(writeOptions.getRetryPolicy(), is(RetryPolicy.DEFAULT));
		assertThat(writeOptions.getConsistencyLevel(), is(nullValue()));
		assertThat(writeOptions.getDriverConsistencyLevel(), is(com.datastax.driver.core.ConsistencyLevel.ANY));
		assertThat(writeOptions.getReadTimeout(), is(1L));
		assertThat(writeOptions.getFetchSize(), is(10));
		assertThat(writeOptions.getTracing(), is(true));
	}

	/**
	 * @see DATACASS-202
	 */
	@Test
	public void buildReadTimeoutOptionsWriteOptions() {

		WriteOptions writeOptions = WriteOptions.builder() //
				.readTimeout(1, TimeUnit.MINUTES)//
				.build(); //

		assertThat(writeOptions.getReadTimeout(), is(60L * 1000L));
		assertThat(writeOptions.getFetchSize(), is(nullValue()));
		assertThat(writeOptions.getTracing(), is(nullValue()));
	}

	/**
	 * @see DATACASS-202
	 */
	@Test
	public void buildQueryOptionsWithDriverRetryPolicy() {

		QueryOptions writeOptions = QueryOptions.builder() //
				.retryPolicy(FallthroughRetryPolicy.INSTANCE) //
				.build(); //

		assertThat(writeOptions.getRetryPolicy(), is(nullValue()));
		assertThat(writeOptions.getDriverRetryPolicy(),
				is(equalTo((com.datastax.driver.core.policies.RetryPolicy) FallthroughRetryPolicy.INSTANCE)));
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

		WriteOptions.builder() //
				.retryPolicy(RetryPolicy.DEFAULT).retryPolicy(FallthroughRetryPolicy.INSTANCE);
	}

	/**
	 * @see DATACASS-202
	 */
	@Test(expected = IllegalStateException.class)
	public void builderShouldRejectSettingDriverAndOurRetryPolicy() {

		WriteOptions.builder() //
				.retryPolicy(FallthroughRetryPolicy.INSTANCE)//
				.retryPolicy(RetryPolicy.DEFAULT);

	}
}
