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

import com.datastax.driver.core.Row;

/**
 * Simple internal callback to allow operations on a {@link Row}.
 *
 * @author Alex Shvid
 * @deprecated as of 2.0. Superseded by {@link RowCallbackHandler}.
 */
public interface RowCallback<T> {

	/**
	 * Implementations must implement this method to process each row of data in the
	 * {@link com.datastax.driver.core.ResultSet}. This method is only supposed to extract values of the current row.
	 *
	 * @param object
	 * @return
	 */
	T doWith(Row object);
}
