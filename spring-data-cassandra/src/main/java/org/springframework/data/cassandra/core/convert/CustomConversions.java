/*
 * Copyright 2016-2021 the original author or authors.
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
package org.springframework.data.cassandra.core.convert;

import java.util.List;

import org.springframework.data.mapping.model.SimpleTypeHolder;

/**
 * Value object to capture custom conversion. That is essentially a {@link List} of converters and some additional logic
 * around them. The converters are pretty much builds up two sets of types which Cassandra basic types can be converted
 * into and from. These types will be considered simple ones (which means they neither need deeper inspection nor nested
 * conversion. Thus the {@link CustomConversions} also act as factory for {@link SimpleTypeHolder}
 *
 * @author Mark Paluch
 * @since 1.5
 * @deprecated since 2.0, use {@link CassandraCustomConversions}.
 */
@Deprecated
public class CustomConversions extends CassandraCustomConversions {

	/**
	 * Create a new {@link CustomConversions} instance registering the given converters.
	 *
	 * @param converters
	 */
	private CustomConversions(List<?> converters) {
		super(converters);
	}
}
