/*
 * Copyright 2011-2013 the original author or authors.
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
package org.springframework.cassandra.core;

import com.datastax.driver.core.DataType;

/**
 * @author David Webb
 * 
 */
public class CqlParameterValue extends CqlParameter {

	private final Object value;

	/**
	 * Create a new CqlParameterValue, supplying the Cassandra DataType.
	 * 
	 * @param type Cassandra Data Type of the parameter according to {@link DataType}
	 * @param value the value object
	 */
	public CqlParameterValue(DataType type, Object value) {
		super(type);
		this.value = value;
	}

	/**
	 * Create a new CqlParameterValue, supplying the Cassandra DataType.
	 * 
	 * @param type Cassandra Data Type of the parameter according to {@link DataType}
	 * @param scale the number of digits after the decimal point (for DECIMAL and NUMERIC types)
	 * @param value the value object
	 */
	public CqlParameterValue(DataType type, int scale, Object value) {
		super(type, scale);
		this.value = value;
	}

	/**
	 * Create a new CqlParameterValue based on the given CqlParameter declaration.
	 * 
	 * @param declaredParam the declared CqlParameter to define a value for
	 * @param value the value object
	 */
	public CqlParameterValue(CqlParameter declaredParam, Object value) {
		super(declaredParam);
		this.value = value;
	}

	/**
	 * Return the value object that this parameter value holds.
	 */
	public Object getValue() {
		return this.value;
	}
}
