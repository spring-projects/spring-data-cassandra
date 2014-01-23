package org.springframework.cassandra.test.integration.support;

import java.io.InputStream;
import java.util.Properties;

@SuppressWarnings("serial")
public class BuildProperties extends Properties {

	public BuildProperties() {
		this("/build.properties");
	}

	public BuildProperties(String resourceName) {
		loadProperties(resourceName);
	}

	public void loadProperties(String resourceName) {
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

	public int getInt(String key) {
		String property = getProperty(key);
		return Integer.parseInt(property);
	}

	public boolean getBoolean(String key) {
		return Boolean.parseBoolean(getProperty(key));
	}
}
