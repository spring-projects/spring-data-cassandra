/*
 * Copyright 2013-2014 the original author or authors.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.cassandra.test.integration.support;

import java.io.InputStream;
import java.util.Properties;

@SuppressWarnings("serial")
public class SpringCqlBuildProperties extends Properties {

	protected String resourceName = null;

	public SpringCqlBuildProperties() {
		this("/" + SpringCqlBuildProperties.class.getName() + ".properties");
	}

	protected SpringCqlBuildProperties(String resourceName) {
		this.resourceName = resourceName;

		loadProperties();
	}

	public void loadProperties() {
		loadProperties(resourceName);
		putAll(System.getProperties());
	}

	protected void loadProperties(String resourceName) {
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

	public int getCassandraPort() {
		return getInt("build.cassandra.native_transport_port");
	}

	public int getCassandraRpcPort() {
		return getInt("build.cassandra.rpc_port");
	}

	public int getCassandraStoragePort() {
		return getInt("build.cassandra.storage_port");
	}

	public int getCassandraSslStoragePort() {
		return getInt("build.cassandra.ssl_storage_port");
	}

	public long getCqlInitializationTimeout() {
		return getLong("build.cql.init.timeout");
	}

	public String getCassandraHost() {
		return getProperty("build.cassandra.host");
	}

	public CassandraType getCassandraType() {
		String property = getProperty("build.cassandra.mode");
		if (property != null && property.equalsIgnoreCase(CassandraType.EXTERNAL.name())) {
			return CassandraType.EXTERNAL;
		}
		return CassandraType.EMBEDDED;
	}

	public int getInt(String key) {
		String property = getProperty(key);
		try {
			return Integer.parseInt(property);
		} catch (NumberFormatException e) {
			throw new IllegalStateException(resourceName + ": Cannot parse property " + key + " to int: " + property);
		}
	}

	public long getLong(String key) {
		String property = getProperty(key);
		try {
			return Long.parseLong(property);
		} catch (NumberFormatException e) {
			throw new IllegalStateException(resourceName + ": Cannot parse property " + key + " to long: " + property + "");
		}
	}

	public boolean getBoolean(String key) {
		String property = getProperty(key);
		try {
			return Boolean.parseBoolean(property);
		} catch (NumberFormatException e) {
			throw new IllegalStateException(resourceName + ": Cannot parse property " + key + " to boolean: " + property);
		}
	}

	public enum CassandraType {
		EMBEDDED, EXTERNAL

	}
}
