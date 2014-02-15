/*
 * Copyright 2013-2014 the original author or authors
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.test.integration.support;

import static org.springframework.cassandra.core.keyspace.DropTableSpecification.dropTable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cassandra.test.integration.AbstractEmbeddedCassandraIntegrationTest;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.test.integration.template.CassandraDataOperationsTest.Config;
import org.springframework.util.Assert;

import com.datastax.driver.core.TableMetadata;

public class AbstractSpringDataEmbeddedCassandraIntegrationTest extends AbstractEmbeddedCassandraIntegrationTest {

	public static List<String> SCRIPT;
	public static List<TableMetadata> TABLES;

	static {
		SpringDataCassandraBuildProperties props = new SpringDataCassandraBuildProperties();
		CASSANDRA_NATIVE_PORT = props.getCassandraPort();
	}

	public Logger log = LoggerFactory.getLogger(getClass());

	@Autowired
	public CassandraOperations template;

	/**
	 * Saves all table metadata, then drops & creates all tables.
	 */
	public void recreateAllTables() {

		saveAllTableMetadata();

		for (TableMetadata table : TABLES) {
			template.execute(dropTable(table.getName()));
			template.execute(table.asCQLQuery());
		}
	}

	public void saveAllTableMetadata() {
		saveAllTableMetadata(false);
	}

	/**
	 * Saves all table metadata statically.
	 */
	public void saveAllTableMetadata(boolean force) {

		if (TABLES != null && !force) {
			return;
		}

		TABLES = new ArrayList<TableMetadata>(template.getSession().getCluster().getMetadata()
				.getKeyspace(Config.KEYSPACE_NAME).getTables());
	}

	public List<String> readScriptLines(String resourceName) {
		return readScriptLines(resourceName);
	}

	/**
	 * Reads the lines from the script referenced by {@link #RESOURCE}.
	 */
	public List<String> readScriptLines(String resourceName, boolean force) throws IOException {

		if (SCRIPT != null && !force) {
			return SCRIPT;
		}

		Assert.hasText(resourceName);

		return SCRIPT = FileUtils.readLines(new ClassPathResource(resourceName).getFile());
	}
}
