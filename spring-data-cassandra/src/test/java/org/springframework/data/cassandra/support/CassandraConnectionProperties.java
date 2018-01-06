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

import java.io.InputStream;
import java.util.Properties;

import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

/**
 * Cassandra connection properties using {@code config/cassandra-connection.properties}. Properties are generated during
 * the build and can be override using system properties.
 *
 * @author Mark Paluch
 */
@SuppressWarnings("serial")
public class CassandraConnectionProperties extends Properties {

	protected String resourceName = null;

	/**
	 * Create a new {@link CassandraConnectionProperties} using properties from
	 * {@code config/cassandra-connection.properties}.
	 */
	public CassandraConnectionProperties() {
		this("/config/cassandra-connection.properties");
	}

	protected CassandraConnectionProperties(String resourceName) {
		this.resourceName = resourceName;
		loadProperties();
	}

	private void loadProperties() {
		loadProperties(resourceName);
		putAll(System.getProperties());
	}

	private void loadProperties(String resourceName) {
		InputStream in = null;
		try {
			in = getClass().getResourceAsStream(resourceName);
			if (in == null) {
				return;
			}
			load(in);
		} catch (Exception x) {
			throw new RuntimeException(x);
		} finally {
			if (in != null) {
				try {
					in.close();
				} catch (Exception e) {
					// gulp
				}
			}
		}
	}

	/**
	 * @return the Cassandra port (native).
	 */
	public int getCassandraPort() {
		return getInt("build.cassandra.native_transport_port");
	}

	/**
	 * @return the Cassandra RPC port
	 */
	public int getCassandraRpcPort() {
		return getInt("build.cassandra.rpc_port");
	}

	/**
	 * @return the Cassandra Storage port
	 */
	public int getCassandraStoragePort() {
		return getInt("build.cassandra.storage_port");
	}

	/**
	 * @return the Cassandra SSL Storage port
	 */
	public int getCassandraSslStoragePort() {
		return getInt("build.cassandra.ssl_storage_port");
	}

	/**
	 * @return the Cassandra hostname
	 */
	public String getCassandraHost() {
		return getProperty("build.cassandra.host");
	}

	/**
	 * @return the Cassandra type (Embedded or External)
	 */
	public CassandraType getCassandraType() {

		String property = getProperty("build.cassandra.mode");
		if (property != null && property.equalsIgnoreCase(CassandraType.EXTERNAL.name())) {
			return CassandraType.EXTERNAL;
		}
		return CassandraType.EMBEDDED;
	}

	/**
	 * Retrieve a property and return its value as {@code int}.
	 *
	 * @param propertyName name of the property, must not be empty and not {@literal null}.
	 * @return the property value
	 */
	public int getInt(String propertyName) {
		return convert(propertyName, Integer.class, Integer::parseInt);
	}

	/**
	 * Retrieve a property and return its value as {@code long}.
	 *
	 * @param propertyName name of the property, must not be empty and not {@literal null}.
	 * @return the property value
	 */
	public long getLong(String propertyName) {
		return convert(propertyName, Long.class, Long::parseLong);
	}

	/**
	 * Retrieve a property and return its value as {@code boolean}.
	 *
	 * @param propertyName name of the property, must not be empty and not {@literal null}.
	 * @return the property value
	 */
	public boolean getBoolean(String propertyName) {
		return convert(propertyName, Boolean.class, Boolean::parseBoolean);
	}

	private <T> T convert(String propertyName, Class<T> type, Converter<String, T> converter) {

		Assert.hasText(propertyName, "PropertyName must not be empty!");

		String propertyValue = getProperty(propertyName);
		try {
			return converter.convert(propertyValue);
		} catch (Exception e) {
			throw new IllegalArgumentException(String.format("%1$s: cannot parse value [%2$s] of property [%3$s] as a [%4$s]",
					resourceName, propertyValue, propertyName, type.getSimpleName()), e);
		}
	}

	public enum CassandraType {
		EMBEDDED, EXTERNAL

	}
}
