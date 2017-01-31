/*
 * Copyright 2013-2017 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.core;

import org.springframework.cassandra.core.RowCallback;
import org.springframework.data.cassandra.convert.CassandraConverter;
import org.springframework.util.Assert;

import com.datastax.driver.core.Row;

/**
 * Simple {@link RowCallback} that will transform a {@link Row} into the given target type using the given
 * {@link CassandraConverter}.
 *
 * @author Alex Shvid
 * @author Matthew T. Adams
 */
public class CassandraConverterRowCallback<T> implements RowCallback<T> {

	private final CassandraConverter reader;
	private final Class<T> type;

	public CassandraConverterRowCallback(CassandraConverter reader, Class<T> type) {

		Assert.notNull(reader, "CassandraConverter must not be null");
		Assert.notNull(type, "Target class must not be null");

		this.reader = reader;
		this.type = type;
	}

	@Override
	public T doWith(Row row) {
		return reader.read(type, row);
	}
}
