/*******************************************************************************
 * Copyright 2013-2014 the original author or authors.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
/*
 * Copyright 2010-2013 the original author or authors.
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
package org.springframework.data.cassandra.mapping;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

import com.datastax.driver.core.DataType;

/**
 * Specifies the Cassandra type of the annotated property.
 * 
 * @author Alex Shvid
 * @author Matthew T. Adams
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
public @interface CassandraType {

	/**
	 * The {@link DataType}.{@link Name} of the property.
	 */
	DataType.Name type();

	/**
	 * If the property is collection-like, then this attribute holds a single {@link DataType}.{@link Name}, representing
	 * the element type of the collection.
	 * <p/>
	 * If the property is map, then this attribute holds exactly two {@link DataType}.{@link Name}s: the first is the key
	 * type, and the second is the value type.
	 * <p/>
	 * If the property is neither collection-like or a map, then this attribute is ignored.
	 */
	DataType.Name[] typeArguments() default {};
}
