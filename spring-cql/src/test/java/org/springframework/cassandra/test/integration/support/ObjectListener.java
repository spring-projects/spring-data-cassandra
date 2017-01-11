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

import org.springframework.cassandra.core.QueryForObjectListener;

/**
 * {@link QueryForObjectListener} suitable for tests.
 * 
 * @author Matthew T. Adams
 * @author David Webb
 * @author Mark Paluch
 */
public class ObjectListener<T> extends CallbackSynchronizationSupport implements QueryForObjectListener<T> {

	private volatile T result;
	private volatile Exception exception;

	/**
	 * Allow instances only using {@link #create()}
	 */
	private ObjectListener() {}

	/**
	 * @return a new {@link ObjectListener}.
	 */
	public static <T> ObjectListener<T> create() {
		return new ObjectListener<T>();
	}

	@Override
	public void onQueryComplete(T result) {

		this.result = result;
		countDown();
	}

	@Override
	public void onException(Exception x) {

		this.exception = x;
		countDown();
	}

	public T getResult() {
		return result;
	}

	public Exception getException() {
		return exception;
	}
}
