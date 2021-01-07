/*
 * Copyright 2020-2021 the original author or authors.
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
package org.springframework.data.cassandra.test.util;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.data.cassandra.support.RandomKeyspaceName;

/**
 * Annotation indicating that the test infrastructure should generate a keyspace for the test container.
 * <p>
 * Fields of test classes can be annotated with {@code @TestKeyspace} to request injection of the
 * {@link com.datastax.oss.driver.api.core.CqlSession} and {@code keyspaceName}.
 *
 * @author Mark Paluch
 * @see CassandraExtension
 */
@Target({ ElementType.TYPE })
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Inherited
public @interface TestKeyspaceName {

	/**
	 * Name of the keyspace. Uses a random name if left empty.
	 *
	 * @see RandomKeyspaceName#create()
	 */
	String value() default "";
}
