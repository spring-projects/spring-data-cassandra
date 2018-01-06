/*
 * Copyright 2013-2018 the original author or authors.
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
package org.springframework.data.cassandra;

import org.springframework.dao.NonTransientDataAccessException;

/**
 * Spring data access exception for when Cassandra schema element being created already exists.
 *
 * @author Matthew T. Adams
 */
public class CassandraSchemaElementExistsException extends NonTransientDataAccessException {

	private static final long serialVersionUID = 7798361273692300162L;

	public enum ElementType {
		KEYSPACE, TABLE, COLUMN, INDEX
	}

	private String elementName;
	private ElementType elementType;

	public CassandraSchemaElementExistsException(String elementName, ElementType elementType, String msg,
			Throwable cause) {
		super(msg, cause);
		this.elementName = elementName;
		this.elementType = elementType;
	}

	public String getElementName() {
		return elementName;
	}

	public ElementType getElementType() {
		return elementType;
	}
}
