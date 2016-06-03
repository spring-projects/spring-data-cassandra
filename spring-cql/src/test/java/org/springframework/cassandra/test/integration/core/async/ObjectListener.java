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
package org.springframework.cassandra.test.integration.core.async;

import org.springframework.cassandra.core.QueryForObjectListener;
import org.springframework.cassandra.support.TestListener;

/**
 * @author Matthew T. Adams
 * @author David Webb
 */
public class ObjectListener<T> extends TestListener implements QueryForObjectListener<T> {

	public volatile T result;
	public volatile Exception exception;

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
}
