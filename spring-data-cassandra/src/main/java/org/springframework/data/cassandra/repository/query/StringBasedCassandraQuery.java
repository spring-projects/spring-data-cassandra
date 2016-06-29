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
package org.springframework.data.cassandra.repository.query;

import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.springframework.cassandra.core.cql.CqlStringUtils;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.util.ClassUtils;

import com.datastax.driver.core.LocalDate;

/**
 * Query to use a plain String to create the {@link Query} to actually execute.
 *
 * @author Matthew Adams
 * @author Mark Paluch
 */
public class StringBasedCassandraQuery extends AbstractCassandraQuery {

	@SuppressWarnings("unchecked")
	private static final Set<Class<?>> STRING_LIKE_PARAMETER_TYPES = new HashSet<Class<?>>(
			Arrays.asList(CharSequence.class, char.class, Character.class, char[].class));

	private static final Pattern PLACEHOLDER = Pattern.compile("\\?(\\d+)");

	protected final String query;

	public StringBasedCassandraQuery(String query, CassandraQueryMethod queryMethod, CassandraOperations operations) {

		super(queryMethod, operations);

		this.query = query;
	}

	public StringBasedCassandraQuery(CassandraQueryMethod queryMethod, CassandraOperations operations) {
		this(queryMethod.getAnnotatedQuery(), queryMethod, operations);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.repository.query.AbstractCassandraQuery#createQuery(org.springframework.data.cassandra.repository.query.CassandraParameterAccessor)
	 */
	@Override
	public String createQuery(CassandraParameterAccessor accessor) {
		return replacePlaceholders(query, accessor);
	}

	private String replacePlaceholders(String input, CassandraParameterAccessor accessor) {

		Matcher matcher = PLACEHOLDER.matcher(input);
		String result = input;

		while (matcher.find()) {
			String group = matcher.group();
			int index = Integer.parseInt(matcher.group(1));
			Object value = getParameterWithIndex(accessor, index);
			String stringValue;

			if (isStringLike(value)) {
				stringValue = String.format("'%s'", CqlStringUtils.escapeSingle(value));
			} else if (isTimestampParameter(value)) {
				stringValue = String.format("%d", ((Date) value).getTime());
			} else if (isDateParameter(value)) {
				stringValue = String.format("'%s'", value);
			} else {
				stringValue = value.toString();
			}

			result = result.replace(group, stringValue);
		}

		return result;
	}

	private boolean isTimestampParameter(Object value) {
		return value instanceof Date;
	}

	private boolean isDateParameter(Object value) {
		return value instanceof LocalDate;
	}

	private boolean isStringLike(Object value) {

		if (value != null) {
			for (Class<?> type : STRING_LIKE_PARAMETER_TYPES) {

				if (ClassUtils.isAssignableValue(type, value)) {
					return true;
				}
			}
		}

		return false;
	}

	private Object getParameterWithIndex(CassandraParameterAccessor accessor, int index) {
		return accessor.getBindableValue(index);
	}
}
