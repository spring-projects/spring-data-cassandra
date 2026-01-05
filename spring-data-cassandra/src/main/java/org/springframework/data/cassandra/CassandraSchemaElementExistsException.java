/*
 * Copyright 2013-present the original author or authors.
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
package org.springframework.data.cassandra;

import java.io.Serial;

import org.springframework.dao.NonTransientDataAccessException;
import org.springframework.lang.Nullable;

/**
 * Spring data access exception for when Cassandra schema element being created already exists.
 *
 * @author Matthew T. Adams
 * @author Mark Paluch
 */
public class CassandraSchemaElementExistsException extends NonTransientDataAccessException {

	@Serial private static final long serialVersionUID = 7798361273692300162L;

	@Deprecated(forRemoval = true)
	public enum ElementType {
		KEYSPACE, TABLE, COLUMN, INDEX
	}

	private String elementName;
	private ElementType elementType;

	/**
	 * Constructor for {@link CassandraSchemaElementExistsException}.
	 *
	 * @param elementName the CQL element name.
	 * @param elementType the CQL element type.
	 * @param msg the detail message.
	 * @param cause the root cause from the underlying data access API.
	 */
	@Deprecated(forRemoval = true)
	public CassandraSchemaElementExistsException(String elementName, ElementType elementType, String msg,
			Throwable cause) {
		super(msg, cause);
		this.elementName = elementName;
		this.elementType = elementType;
	}

	/**
	 * Constructor for {@link CassandraSchemaElementExistsException}.
	 *
	 * @param msg the detail message.
	 * @param cause the root cause from the underlying data access API.
	 */
	public CassandraSchemaElementExistsException(String msg, Throwable cause) {
		super(msg, cause);
	}

	@Deprecated(forRemoval = true)
	@Nullable
	public String getElementName() {
		return elementName;
	}

	@Deprecated(forRemoval = true)
	@Nullable
	public ElementType getElementType() {
		return elementType;
	}
}
