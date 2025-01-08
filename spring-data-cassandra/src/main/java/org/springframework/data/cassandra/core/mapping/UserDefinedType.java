/*
 * Copyright 2016-2025 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.core.mapping;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Identifies a domain object as Cassandra User Defined type (UDT). User defined types may contain nested user defined
 * types.
 *
 * @author Fabio J. Mendes
 * @author Mark Paluch
 * @since 1.5
 */
@Documented
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.TYPE })
public @interface UserDefinedType {

	/**
	 * The keyspace for the UDT; must be a valid CQL identifier/quoted identifier or empty to use the session-keyspace.
	 * The value can be either a string or a {@link org.springframework.data.expression.ValueExpression Value Expression}.
	 *
	 * @since 4.4
	 */
	String keyspace() default "";

	/**
	 * The name of the UDT. Must be a valid CQL identifier or quoted identifier. The value can be either a string or a
	 * {@link org.springframework.data.expression.ValueExpression Value Expression}.
	 */
	String value() default "";

	/**
	 * Whether to cause the UDT name to be force-quoted.
	 *
	 * @deprecated since 3.0. The type name gets converted into {@link com.datastax.oss.driver.api.core.CqlIdentifier}
	 *             hence it no longer requires an indication whether the name should be quoted.
	 * @see com.datastax.oss.driver.api.core.CqlIdentifier#fromInternal(String)
	 */
	@Deprecated
	boolean forceQuote() default false;
}
