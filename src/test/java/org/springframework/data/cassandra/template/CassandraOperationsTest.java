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
package org.springframework.data.cassandra.template;

import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.List;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.cassandraunit.CassandraCQLUnit;
import org.cassandraunit.DataLoader;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.cassandraunit.dataset.yaml.ClassPathYamlDataSet;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.cassandra.config.TestConfig;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.core.RingMember;
import org.springframework.data.cassandra.table.Book;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

/**
 * @author David Webb
 * 
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = { TestConfig.class }, loader = AnnotationConfigContextLoader.class)
public class CassandraOperationsTest {

	@Autowired
	private CassandraOperations cassandraTemplate;

	private static Logger log = LoggerFactory.getLogger(CassandraOperationsTest.class);

	private final static String KEYSPACE_NAME = "test";

	@Rule
	public CassandraCQLUnit cassandraCQLUnit = new CassandraCQLUnit(new ClassPathCQLDataSet("cql-dataload.cql", "test"),
			"cassandra.yaml", "localhost", 9042);

	@BeforeClass
	public static void startCassandra() throws IOException, TTransportException, ConfigurationException,
			InterruptedException {

		EmbeddedCassandraServerHelper.startEmbeddedCassandra("cassandra.yaml");

		/*
		 * Load data file to creat the test keyspace before we init the template
		 */
		DataLoader dataLoader = new DataLoader("Test Cluster", "localhost:9160");
		dataLoader.load(new ClassPathYamlDataSet("cassandra-keyspace.yaml"));
	}

	@Before
	public void setupKeyspace() {

		/*
		 * Load data file to creat the test keyspace before we init the template
		 */
		// DataLoader dataLoader = new DataLoader("Test Cluster", "localhost:9160");
		// dataLoader.load(new ClassPathYamlDataSet("cassandra-keyspace.yaml"));

		log.info("Creating Table...");
		createTables();

	}

	private void createTables() {

		// cassandraTemplate
		// .executeQuery("create table users (username text, firstName text, lastName text, PRIMARY KEY (username));");

		// cassandraCQLUnit.
	}

	@Test
	public void ringTest() {

		List<RingMember> ring = cassandraTemplate.describeRing();

		/*
		 * There must be 1 node in the cluster if the embedded server is
		 * running.
		 */
		assertNotNull(ring);

		for (RingMember h : ring) {
			log.info(h.address);
		}
	}

	@Test
	public void insertTest() {

		/*
		 * Test Single Insert
		 */
		Book b = new Book();
		b.setIsbn("123456");
		b.setTitle("Spring Data Cassandra Guide");
		b.setAuthor("Cassandra Guru");
		b.setPages(521);

		cassandraTemplate.insert(b);

		b.setPages(245);

		cassandraTemplate.update(b);

	}

	@After
	public void clearCassandra() {
		EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();

	}

	@AfterClass
	public static void stopCassandra() {
		EmbeddedCassandraServerHelper.stopEmbeddedCassandra();
	}
}
