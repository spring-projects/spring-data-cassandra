/*
 * Copyright 2013-2021 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.repository;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.data.annotation.QueryAnnotation;

/**
 * Annotation to declare finder queries directly on repository methods.
 *
 * @author Alex Shvid
 * @author Matthew T. Adams
 * @author Mark Paluch
 */
@Documented
@Target({ ElementType.ANNOTATION_TYPE, ElementType.METHOD })
@Retention(RetentionPolicy.RUNTIME)
@QueryAnnotation
public @interface Query {

	/**
	 * A Cassandra CQL3 string to define the actual query to be executed. Placeholders {@code ?0}, {@code ?1}, etc are
	 * supported.
	 */
	String value() default "";

	/**
	 * Specifies whether to allow filtering using query derivation without a {@link #value() string query}.
	 *
	 * @since 2.0
	 */
	boolean allowFiltering() default false;

	/**
	 * Specifies whether the {@link #value() CQL query} is
	 * {@link com.datastax.oss.driver.api.core.cql.Statement#isIdempotent}. {@code SELECT} statements are considered
	 * {@link Idempotency#IDEMPOTENT idempotent} by default.
	 *
	 * @since 2.2
	 */
	Idempotency idempotent() default Idempotency.UNDEFINED;

	/**
	 * Returns whether the defined query should be executed as a count projection.
	 *
	 * @since 2.1
	 */
	boolean count() default false;

	/**
	 * Returns whether the defined query should be executed as an exists projection.
	 *
	 * @since 2.1
	 */
	boolean exists() default false;

	/**
	 * Enumeration to define statement idempotency.
	 *
	 * @since 2.2
	 */
	enum Idempotency {

		/**
		 * Undefined state (default for all non-{@code SELECT} statements. Leaves
		 * {@link com.datastax.oss.driver.api.core.cql.Statement#setIdempotent(boolean)} state unchanged.
		 */
		UNDEFINED,

		/**
		 * Statement considered idempotent.
		 */
		IDEMPOTENT,

		/**
		 * Statement considered non-idempotent. Sets
		 * {@link com.datastax.oss.driver.api.core.cql.Statement#setIdempotent(boolean)} to {@code false}.
		 */
		NON_IDEMPOTENT
	}
}
