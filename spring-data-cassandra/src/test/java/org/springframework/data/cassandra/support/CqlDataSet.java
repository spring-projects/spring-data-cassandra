/*
 * Copyright 2017-2024 the original author or authors.
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
package org.springframework.data.cassandra.support;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.List;

import org.springframework.util.Assert;

import com.datastax.oss.driver.shaded.guava.common.io.Resources;

/**
 * An executable CQL data set. The data set can be created from class path resources and execution can be bound to a
 * particular keyspace.
 *
 * @author Mark Paluch
 * @author Mikhail Polivakha
 */
public class CqlDataSet {

	private URL location = null;

	private String keyspaceName = null;

	private CqlDataSet(URL location, String keyspaceName) {

		this.location = location;
		this.keyspaceName = keyspaceName;
	}

	/**
	 * Obtain the {@link List} of statements to execute.
	 *
	 * @return
	 */
	public List<String> getCqlStatements() {
		return getLines();
	}

	private List<String> getLines() {
		try {
			return Resources.readLines(location, Charset.defaultCharset());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Returns the optional keyspace name.
	 *
	 * @return
	 */
	public String getKeyspaceName() {
		return keyspaceName;
	}

	/**
	 * Bind the {@link CqlDataSet} to a particular keyspace. Create a new instance of the {@link CqlDataSet} with the
	 * keyspace name set.
	 *
	 * @param keyspaceName
	 * @return
	 */
	public CqlDataSet executeIn(String keyspaceName) {

		Assert.hasText(keyspaceName, "KeyspaceName must not be empty");
		return new CqlDataSet(location, keyspaceName);
	}

	/**
	 * Create a {@link CqlDataSet} from a class-path resource.
	 *
	 * @param resource
	 * @return
	 */
	public static CqlDataSet fromClassPath(String resource) {

		URL url = Resources.getResource(resource);
		return new CqlDataSet(url, null);
	}

	/**
	 * Create a {@link CqlDataSet} from a class-path resource.
	 */
	public static CqlDataSet fromClassPath(String resource, String keyspaceName) {

		URL url = Resources.getResource(resource);
		return new CqlDataSet(url, keyspaceName);
	}
}
