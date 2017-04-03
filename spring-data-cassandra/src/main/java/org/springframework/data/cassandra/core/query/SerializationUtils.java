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
package org.springframework.data.cassandra.core.query;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.springframework.core.convert.converter.Converter;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.TypeCodec;

/**
 * Utility methods for CQL serialization.
 *
 * @author Mark Paluch
 * @since 2.0
 */
abstract class SerializationUtils {

	private SerializationUtils() {}

	/**
	 * Serializes the given object into pseudo-CQL meaning it's trying to create a CQL representation as far as possible
	 * but falling back to the given object's {@link Object#toString()} method if it's not serializable. Useful for
	 * printing raw {@link Criteria}s containing complex values before actually converting them into Mongo native types.
	 *
	 * @param criteria
	 * @return
	 */
	public static String serializeToCqlSafely(CriteriaDefinition criteria) {

		if (criteria == null) {
			return null;
		}

		CriteriaDefinition.Predicate predicate = criteria.getPredicate();
		return serialize(criteria.getColumnName(), criteria.getPredicate().getOperator())
				.append(serializeToCqlSafely(predicate.getValue())).toString();

	}

	/**
	 * Serializes the given object into pseudo-CQL meaning it's trying to create a CQL representation as far as possible
	 * but falling back to the given object's {@link Object#toString()} method if it's not serializable. Useful for
	 * printing raw {@link Criteria}s containing complex values before actually converting them into Mongo native types.
	 *
	 * @param criteria
	 * @return
	 */
	public static String serializeToCqlSafely(Object value) {

		if (value == null) {
			return null;
		}

		try {
			return serialize(value);
		} catch (Exception e) {
			if (value instanceof Set) {
				return toString((Set<?>) value);
			} else if (value instanceof Collection) {
				return toString((Collection<?>) value);
			} else if (value instanceof Map) {
				return toString((Map<?, ?>) value);
			} else {
				return value.toString();
			}
		}
	}

	private static String serialize(Object value) {

		if (value == null) {
			return "null";
		}

		TypeCodec<Object> codec = CodecRegistry.DEFAULT_INSTANCE.codecFor(value);
		return codec.format(value);
	}

	private static StringBuilder serialize(ColumnName key, String operator) {

		StringBuilder builder = new StringBuilder(16);

		return builder.append(key).append(' ').append(operator).append(' ');
	}

	private static String toString(Map<?, ?> source) {

		return iterableToDelimitedString(source.entrySet(), "{ ", " }",
				s -> String.format("%s : %s", serialize(s.getKey()), serialize(s.getValue())));
	}

	private static String toString(Set<?> source) {
		return iterableToDelimitedString(source, "{", "}",
				(Converter<Object, Object>) SerializationUtils::serializeToCqlSafely);
	}

	private static String toString(Collection<?> source) {
		return iterableToDelimitedString(source, "[", "]",
				(Converter<Object, Object>) SerializationUtils::serializeToCqlSafely);
	}

	/**
	 * Creates a string representation from the given {@link Iterable} prepending the prefix, applying the given
	 * {@link Converter} to each element before adding it to the result {@link String}, concatenating each element with
	 * {@literal ,} and applying the postfix.
	 */
	private static <T> String iterableToDelimitedString(Iterable<T> source, String prefix, String postfix,
			Converter<? super T, Object> transformer) {

		StringBuilder builder = new StringBuilder(prefix);
		Iterator<T> iterator = source.iterator();

		while (iterator.hasNext()) {
			builder.append(transformer.convert(iterator.next()));
			if (iterator.hasNext()) {
				builder.append(",");
			}
		}

		return builder.append(postfix).toString();
	}
}
