/*
 * Copyright 2017-2018 the original author or authors.
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
package org.springframework.data.cassandra.support;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.springframework.data.cassandra.core.cql.CqlIdentifier;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.UserType;
import com.datastax.driver.core.UserType.Field;

/**
 * @author Mark Paluch
 */
public class UserTypeBuilder {

	private final CqlIdentifier typeName;
	private List<Field> fields = new ArrayList<>();

	private UserTypeBuilder(CqlIdentifier typeName) {
		this.typeName = typeName;
	}

	public static UserTypeBuilder forName(String typeName) {
		return forName(CqlIdentifier.of(typeName));
	}

	public static UserTypeBuilder forName(CqlIdentifier typeName) {
		return new UserTypeBuilder(typeName);
	}

	public UserTypeBuilder withField(String fieldName, DataType dataType) {
		this.fields.add(createField(fieldName, dataType));
		return this;
	}

	public UserType build() {
		return createUserType(this.typeName.getUnquoted(), fields);
	}

	private Field createField(String fieldName, DataType dataType) {

		try {
			Constructor<Field> constructor = Field.class.getDeclaredConstructor(String.class, DataType.class);
			constructor.setAccessible(true);
			return constructor.newInstance(fieldName, dataType);
		} catch (Exception e) {
			throw new IllegalStateException(e);
		}
	}

	private UserType createUserType(String typeName, Collection<Field> fields) {

		try {
			Constructor<UserType> constructor = UserType.class.getDeclaredConstructor(String.class, String.class,
					Boolean.TYPE, Collection.class, ProtocolVersion.class, CodecRegistry.class);
			constructor.setAccessible(true);
			return constructor.newInstance(typeName, typeName, false, fields, ProtocolVersion.NEWEST_SUPPORTED,
					CodecRegistry.DEFAULT_INSTANCE);
		} catch (Exception e) {
			throw new IllegalStateException(e);
		}
	}
}
