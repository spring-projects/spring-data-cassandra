/*
 * Copyright 2025 the original author or authors.
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
 * Specific variant of {@code CassandraType(VECTOR)} allowing specification of a Cassandra vector type alongside with
 * its subtype and number of dimensions.
 *
 * @author Mark Paluch
 * @since 4.5
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(value = { ElementType.ANNOTATION_TYPE, ElementType.FIELD, ElementType.METHOD, ElementType.PARAMETER })
@CassandraType(type = CassandraType.Name.VECTOR)
public @interface VectorType {

	/**
	 * @return Vector subtype, defaults to float.
	 */
	CassandraType.Name subtype() default CassandraType.Name.FLOAT;

	/**
	 * @return number of dimensions.
	 */
	int dimensions();

}
