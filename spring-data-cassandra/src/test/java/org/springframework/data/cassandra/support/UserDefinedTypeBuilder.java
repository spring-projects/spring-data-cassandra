/*
 * Copyright 2017-present the original author or authors.
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
package org.springframework.data.cassandra.support;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.internal.core.type.DefaultUserDefinedType;

/**
 * Builder for {@link UserDefinedType}. Intended for internal usage during testing.
 *
 * @author Mark Paluch
 */
public class UserDefinedTypeBuilder {

	private final CqlIdentifier typeName;
	private Map<CqlIdentifier, DataType> fields = new LinkedHashMap<>();

	private UserDefinedTypeBuilder(CqlIdentifier typeName) {
		this.typeName = typeName;
	}

	public static UserDefinedTypeBuilder forName(String typeName) {
		return forName(CqlIdentifier.fromCql(typeName));
	}

	private static UserDefinedTypeBuilder forName(CqlIdentifier typeName) {
		return new UserDefinedTypeBuilder(typeName);
	}

	public UserDefinedTypeBuilder withField(String fieldName, DataType dataType) {
		this.fields.put(CqlIdentifier.fromCql(fieldName), dataType);
		return this;
	}

	public UserDefinedType build() {

		DefaultUserDefinedType type = new DefaultUserDefinedType(CqlIdentifier.fromCql("system"), this.typeName, false,
				new ArrayList<>(fields.keySet()), new ArrayList<>(fields.values()));
		return type;
	}

}
