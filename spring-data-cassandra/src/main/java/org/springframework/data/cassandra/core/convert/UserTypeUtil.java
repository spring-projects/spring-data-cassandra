/*
 * Copyright 2020 the original author or authors.
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

import org.springframework.util.Assert;

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.api.core.type.SetType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;

/**
 * {@link com.datastax.driver.core.UserType} utility methods. Mainly for internal use within the framework.
 *
 * @author Mark Paluch
 * @since 2.0
 */
class UserTypeUtil {

	/**
	 * Potentially create a frozen variant of {@code dataType}. Frozen types are required for nested UDTs (a user-type
	 * referencing another user-type) or UDTs within collection types.
	 *
	 * @param dataType must not be {@literal null}.
	 * @return the potentially frozen {@link DataType}.
	 */
	static DataType potentiallyFreeze(DataType dataType) {

		Assert.notNull(dataType, "DataType must not be null");

		if (dataType instanceof ListType) {

			ListType collectionType = (ListType) dataType;
			DataType elementType = collectionType.getElementType();

			if (isCollectionType(elementType) || isNonFrozenUdt(elementType)) {
				return DataTypes.listOf(potentiallyFreeze(elementType), collectionType.isFrozen());
			}
		}

		if (dataType instanceof SetType) {

			SetType collectionType = (SetType) dataType;
			DataType elementType = collectionType.getElementType();

			if (isCollectionType(elementType) || isNonFrozenUdt(elementType)) {
				return DataTypes.setOf(potentiallyFreeze(elementType), collectionType.isFrozen());
			}
		}

		if (dataType instanceof MapType) {

			MapType collectionType = (MapType) dataType;

			DataType keyType = collectionType.getKeyType();
			DataType valueType = collectionType.getValueType();

			if (isCollectionType(keyType) || isCollectionType(valueType) || isNonFrozenUdt(keyType)
					|| isNonFrozenUdt(valueType)) {
				return DataTypes.mapOf(potentiallyFreeze(keyType), potentiallyFreeze(valueType), collectionType.isFrozen());
			}
		}

		if (isNonFrozenUdt(dataType)) {
			return ((UserDefinedType) dataType).copy(true);
		}

		return dataType;
	}

	private static boolean isCollectionType(DataType typeArgument) {
		return typeArgument instanceof ListType || typeArgument instanceof SetType || typeArgument instanceof MapType;
	}

	private static boolean isNonFrozenUdt(DataType dataType) {
		return dataType instanceof UserDefinedType && !((UserDefinedType) dataType).isFrozen();
	}
}
