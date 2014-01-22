package org.springframework.cassandra.test.integration.support;

import java.io.InputStream;
import java.util.Properties;

@SuppressWarnings("serial")
public class BuildProperties extends Properties {

	public BuildProperties() {
		this("build.properties");
	}

	public BuildProperties(String resourceName) {

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

	public int getInt(String key) {
		String property = getProperty(key);
		return Integer.parseInt(property);
	}

	public boolean getBoolean(String key) {
		return Boolean.parseBoolean(getProperty(key));
	}
}
