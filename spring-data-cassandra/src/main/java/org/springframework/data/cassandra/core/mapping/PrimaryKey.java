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

import org.springframework.data.annotation.Id;

/**
 * Identifies the primary key field of the entity, which may be of a basic type or of a type that represents a composite
 * primary key class. This field corresponds to the {@code PRIMARY KEY} of the corresponding Cassandra table. Only one
 * field in a given type hierarchy may be annotated with this annotation.
 * <p/>
 * Remember, if the Cassandra table has multiple primary key columns, then you must define a class annotated with
 * {@link PrimaryKeyClass} to represent the primary key!
 * <p/>
 * Use {@link PrimaryKeyColumn} in conjunction with {@link Id} to specify extended primary key column properties.
 *
 * @author Alex Shvid
 * @author Matthew T. Adams
 * @author Mark Paluch
 * @see Id
 * @see PrimaryKeyColumn
 */
@Documented
@Retention(value = RetentionPolicy.RUNTIME)
@Target(value = { ElementType.ANNOTATION_TYPE, ElementType.FIELD, ElementType.METHOD })
@Id
public @interface PrimaryKey {

	/**
	 * The column name for the primary key if it is of a simple type, else ignored.
	 */
	String value() default "";

	/**
	 * Whether to cause the column name to be force-quoted if the primary key is of a simple type, else ignored.
	 *
	 * @deprecated since 3.0. The column name gets converted into {@link com.datastax.oss.driver.api.core.CqlIdentifier}
	 *             hence it no longer requires an indication whether the name should be quoted.
	 * @see com.datastax.oss.driver.api.core.CqlIdentifier#fromInternal(String)
	 */
	@Deprecated
	boolean forceQuote() default false;
}
