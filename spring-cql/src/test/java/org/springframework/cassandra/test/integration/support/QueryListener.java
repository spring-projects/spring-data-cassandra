/*
 * Copyright 2016-2017 the original author or authors.
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
package org.springframework.cassandra.test.integration.support;

import org.springframework.cassandra.core.AsynchronousQueryListener;

import com.datastax.driver.core.ResultSetFuture;

/**
 * {@link AsynchronousQueryListener} suitable for usage in tests.
 * 
 * @author Matthew T. Adams
 * @author David Webb
 * @author Mark Paluch
 */
public class QueryListener extends CallbackSynchronizationSupport implements AsynchronousQueryListener {

	private volatile ResultSetFuture resultSetFuture;

	/**
	 * Allow instances only using {@link #create()}
	 */
	private QueryListener() {}

	/**
	 * @return a new {@link QueryListener}.
	 */
	public static QueryListener create() {
		return new QueryListener();
	}

	@Override
	public void onQueryComplete(ResultSetFuture resultSetFuture) {

		this.resultSetFuture = resultSetFuture;
		countDown();
	}

	public ResultSetFuture getResultSetFuture() {
		return resultSetFuture;
	}
}
