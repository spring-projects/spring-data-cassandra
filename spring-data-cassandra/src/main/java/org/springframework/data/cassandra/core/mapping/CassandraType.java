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
package org.springframework.data.cassandra.core.mapping;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Specifies the Cassandra type of the annotated property or parameter when used in query methods.
 *
 * @author Alex Shvid
 * @author Matthew T. Adams
 * @author Mark Paluch
 * @see Name
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(value = { ElementType.TYPE, ElementType.TYPE_PARAMETER, ElementType.ANNOTATION_TYPE, ElementType.FIELD,
		ElementType.METHOD, ElementType.PARAMETER })
public @interface CassandraType {

	/**
	 * The {@link Name} of the property.
	 */
	Name type();

	/**
	 * If the property is {@link java.util.Collection Collection-like}, then this attribute holds a single {@link Name
	 * DataType Name} representing the element type of the {@link java.util.Collection}.
	 * <p/>
	 * If the property is a {@link java.util.Map}, then this attribute holds exactly two {@link Name DataType Names}; the
	 * first is the key type and the second is the value type.
	 * <p/>
	 * If the property is neither {@link java.util.Collection Collection-like} nor a {@link java.util.Map}, then this
	 * attribute is ignored.
	 *
	 * @return an array of {@link Name} objects.
	 * @see Name
	 */
	Name[] typeArguments() default {};

	/**
	 * If the property maps to a User-Defined Type (UDT) then this attribute holds the user type name. For
	 * {@link java.util.Collection Collection-like} properties the user type name applies to the component type. For
	 * {@link java.util.Map} properties, {@link #typeArguments()} configured to {@link Name#UDT} are resolved using the
	 * user type name. The user type name is only required if the UDT does not map to a class annotated with
	 * {@link UserDefinedType}.
	 *
	 * @return {@link String name} of the user type
	 * @since 1.5
	 */
	String userTypeName() default "";

	/**
	 * Cassandra Protocol types.
	 *
	 * @since 3.0
	 */
	enum Name {
		ASCII, BIGINT, BLOB, BOOLEAN, COUNTER, DECIMAL, DOUBLE, FLOAT, INT, TIMESTAMP, UUID, VARCHAR, TEXT, VARINT, TIMEUUID, INET, DATE, TIME, SMALLINT, TINYINT, DURATION, LIST, MAP, SET, UDT, TUPLE;
	}
}
