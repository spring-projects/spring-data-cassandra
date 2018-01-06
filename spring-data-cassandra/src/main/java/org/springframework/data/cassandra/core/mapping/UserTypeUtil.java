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
package org.springframework.data.cassandra.core.mapping;

import static org.springframework.data.cassandra.core.cql.CqlIdentifier.*;

import org.springframework.data.cassandra.core.cql.CqlIdentifier;
import org.springframework.util.Assert;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.DataType.CollectionType;
import com.datastax.driver.core.DataType.Name;
import com.datastax.driver.core.UserType;

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

		if (dataType.getName() == Name.LIST && dataType instanceof CollectionType) {

			CollectionType collectionType = (CollectionType) dataType;

			DataType typeArgument = collectionType.getTypeArguments().get(0);

			if (typeArgument instanceof CollectionType || isNonFrozenUdt(typeArgument)) {
				return DataType.list(potentiallyFreeze(typeArgument), collectionType.isFrozen());
			}
		}

		if (dataType.getName() == Name.SET && dataType instanceof CollectionType) {

			CollectionType collectionType = (CollectionType) dataType;

			DataType typeArgument = collectionType.getTypeArguments().get(0);

			if (typeArgument instanceof CollectionType || isNonFrozenUdt(typeArgument)) {
				return DataType.set(potentiallyFreeze(typeArgument), collectionType.isFrozen());
			}
		}

		if (dataType.getName() == Name.MAP && dataType instanceof CollectionType) {

			CollectionType collectionType = (CollectionType) dataType;

			DataType keyType = collectionType.getTypeArguments().get(0);
			DataType valueType = collectionType.getTypeArguments().get(1);

			if (keyType instanceof CollectionType || valueType instanceof CollectionType || isNonFrozenUdt(keyType)
					|| isNonFrozenUdt(valueType)) {
				return DataType.map(potentiallyFreeze(keyType), potentiallyFreeze(valueType), collectionType.isFrozen());
			}
		}

		return isNonFrozenUdt(dataType) ? new FrozenLiteralDataType(getTypeName(dataType)) : dataType;
	}

	private static CqlIdentifier getTypeName(DataType dataType) {

		if (dataType instanceof UserType) {
			return CqlIdentifier.of(((UserType) dataType).getTypeName());
		}

		return of(dataType.asFunctionParameterString());
	}

	private static boolean isNonFrozenUdt(DataType dataType) {
		return dataType.getName() == Name.UDT && !dataType.isFrozen();
	}

	/**
	 * @author Jens Schauder
	 * @since 1.5.1
	 */
	static class FrozenLiteralDataType extends DataType {

		private final CqlIdentifier type;

		FrozenLiteralDataType(CqlIdentifier type) {

			super(Name.UDT);

			this.type = type;
		}

		/* (non-Javadoc)
		 * @see com.datastax.driver.core.DataType#isFrozen()
		 */
		@Override
		public boolean isFrozen() {
			return true;
		}

		/* (non-Javadoc)
		 * @see java.lang.Object#toString()
		 */
		@Override
		public String toString() {
			return String.format("frozen<%s>", type.toCql());
		}
	}
}
