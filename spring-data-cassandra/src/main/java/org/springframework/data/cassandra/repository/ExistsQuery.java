/*
 * Copyright 2018-2021 the original author or authors.
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
package org.springframework.data.cassandra.repository;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.core.annotation.AliasFor;

/**
 * Annotation to declare exists queries directly on repository methods. Both attributes allow using a placeholder
 * notation of {@code ?0}, {@code ?1} and so on.
 *
 * @author Mark Paluch
 * @since 2.1
 */
@Documented
@Target({ ElementType.METHOD, ElementType.ANNOTATION_TYPE })
@Retention(RetentionPolicy.RUNTIME)
@Query(exists = true)
public @interface ExistsQuery {

	/**
	 * A Cassandra CQL3 string to define the actual query to be executed. Placeholders {@code ?0}, {@code ?1}, etc are
	 * supported.
	 */
	@AliasFor(annotation = Query.class)
	String value() default "";

}
