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

import java.util.LinkedList;
import java.util.List;

import org.springframework.util.Assert;

import com.datastax.driver.core.DataType;

/**
 * @author David Webb
 * 
 */
public class CqlParameter {

	/** The name of the parameter, if any */
	private String name;

	/** SQL type constant from {@link DataType} */
	private final DataType type;

	/** The scale to apply in case of a NUMERIC or DECIMAL type, if any */
	private Integer scale;

	/**
	 * Create a new anonymous CqlParameter, supplying the SQL type.
	 * 
	 * @param type Cassandra Data Type of the parameter according to {@link DataType}
	 */
	public CqlParameter(DataType type) {
		this.type = type;
	}

	/**
	 * Create a new anonymous CqlParameter, supplying the SQL type.
	 * 
	 * @param type Cassandra Data Type of the parameter according to {@link DataType}
	 * @param scale the number of digits after the decimal point
	 */
	public CqlParameter(DataType type, int scale) {
		this.type = type;
		this.scale = scale;
	}

	/**
	 * Create a new CqlParameter, supplying name and SQL type.
	 * 
	 * @param name name of the parameter, as used in input and output maps
	 * @param type Cassandra Data Type of the parameter according to {@link DataType}
	 */
	public CqlParameter(String name, DataType type) {
		this.name = name;
		this.type = type;
	}

	/**
	 * Create a new CqlParameter, supplying name and SQL type.
	 * 
	 * @param name name of the parameter, as used in input and output maps
	 * @param type Cassandra Data Type of the parameter according to {@link DataType}
	 * @param scale the number of digits after the decimal point (for DECIMAL and NUMERIC types)
	 */
	public CqlParameter(String name, DataType type, int scale) {
		this.name = name;
		this.type = type;
		this.scale = scale;
	}

	/**
	 * Copy constructor.
	 * 
	 * @param otherParam the CqlParameter object to copy from
	 */
	public CqlParameter(CqlParameter otherParam) {
		Assert.notNull(otherParam, "CqlParameter object must not be null");
		this.name = otherParam.name;
		this.type = otherParam.type;
		this.scale = otherParam.scale;
	}

	/**
	 * Return the name of the parameter.
	 */
	public String getName() {
		return this.name;
	}

	/**
	 * Return the SQL type of the parameter.
	 */
	public DataType getType() {
		return this.type;
	}

	/**
	 * Return the scale of the parameter, if any.
	 */
	public Integer getScale() {
		return this.scale;
	}

	/**
	 * Return whether this parameter holds input values that should be set before execution even if they are {@code null}.
	 * <p>
	 * This implementation always returns {@code true}.
	 */
	public boolean isInputValueProvided() {
		return true;
	}

	/**
	 * Convert a list of JDBC types, as defined in {@code java.sql.Types}, to a List of CqlParameter objects as used in
	 * this package.
	 */
	public static List<CqlParameter> sqlTypesToAnonymousParameterList(DataType[] types) {
		List<CqlParameter> result = new LinkedList<CqlParameter>();
		if (types != null) {
			for (DataType type : types) {
				result.add(new CqlParameter(type));
			}
		}
		return result;
	}
}
