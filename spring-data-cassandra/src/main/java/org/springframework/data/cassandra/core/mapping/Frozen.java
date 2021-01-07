/*
 * Copyright 2020-2021 the original author or authors.
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
 * Indicates that a persistent property should use a {@code frozen} column type.
 *
 * @author Jens Schauder
 * @see <a href="https://express-cassandra.readthedocs.io/en/stable/advanced/#frozen-collections">Documentation about
 *      frozen collections</a>
 * @since 3.0
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(value = { ElementType.TYPE_USE, ElementType.FIELD, ElementType.METHOD })
public @interface Frozen {

}
