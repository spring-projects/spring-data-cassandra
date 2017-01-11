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

import java.util.Map;

import org.springframework.cassandra.core.QueryForMapListener;

/**
 * {@link QueryForMapListener} suitable for tests.
 * 
 * @author Matthew T. Adams
 * @author Mark Paluch
 */
public class MapListener extends CallbackSynchronizationSupport implements QueryForMapListener {

	private volatile Map<String, Object> result;
	private volatile Exception exception;

	/**
	 * Allow instances only using {@link #create()}
	 */
	private MapListener() {}

	/**
	 * @return a new {@link MapListener}.
	 */
	public static MapListener create() {
		return new MapListener();
	}

	@Override
	public void onQueryComplete(Map<String, Object> results) {

		this.result = results;
		countDown();
	}

	@Override
	public void onException(Exception x) {

		this.exception = x;
		countDown();
	}

	public Map<String, Object> getResult() {
		return result;
	}

	public Exception getException() {
		return exception;
	}
}
