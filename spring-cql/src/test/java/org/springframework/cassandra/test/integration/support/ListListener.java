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

import java.util.List;

import org.springframework.cassandra.core.QueryForListListener;

/**
 * {@link QueryForListListener} suitable for tests.
 * 
 * @author Matthew T. Adams
 * @author David Webb
 */
public class ListListener<T> extends CallbackSynchronizationSupport implements QueryForListListener<T> {

	private volatile Exception exception;
	private volatile List<T> result;

	/**
	 * Allow instances only using {@link #create()}
	 */
	private ListListener() {}

	/**
	 * @return a new {@link QueryForListListener}.
	 */
	public static <T> ListListener<T> create() {
		return new ListListener<T>();
	}

	@Override
	public void onQueryComplete(List<T> results) {

		this.result = results;
		countDown();
	}

	@Override
	public void onException(Exception x) {

		this.exception = x;
		countDown();
	}

	public Exception getException() {
		return exception;
	}

	public List<T> getResult() {
		return result;
	}
}
