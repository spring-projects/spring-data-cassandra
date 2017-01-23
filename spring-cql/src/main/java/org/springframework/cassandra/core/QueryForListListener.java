/*
 * Copyright 2017 the original author or authors.
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

import java.util.List;

import com.datastax.driver.core.ResultSet;

/**
 * Listener used to receive asynchronous results expected as a {@code List<T>}.
 *
 * @author Matthew T. Adams
 * @param <T>
 */
public interface QueryForListListener<T> {

	/**
	 * Called upon query completion.
	 */
	void onQueryComplete(List<T> results);

	/**
	 * Called if an exception is raised while getting or converting the {@link ResultSet}.
	 */
	void onException(Exception x);
}
