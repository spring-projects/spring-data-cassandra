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
import java.util.Date;
import java.util.List;

import junit.framework.Assert;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.cassandraunit.CassandraCQLUnit;
import org.cassandraunit.DataLoader;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.cassandraunit.dataset.yaml.ClassPathYamlDataSet;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
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
import org.springframework.data.cassandra.table.LogEntry;
import org.springframework.data.cassandra.table.User;
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
	public void UsersTest() {

		User u = new User();
		u.setUsername("cassandra");
		u.setFirstName("Apache");
		u.setLastName("Cassnadra");
		u.setAge(40);

		cassandraTemplate.insert(u, "users");

		User us = cassandraTemplate.selectOne("select * from test.users where username='cassandra';", User.class);

		log.debug("Output from select One");
		log.debug(us.getFirstName());
		log.debug(us.getLastName());

		List<User> users = cassandraTemplate.select("Select * from test.users", User.class);

		log.debug("Output from select All");
		for (User x : users) {
			log.debug(x.getFirstName());
			log.debug(x.getLastName());
		}

		cassandraTemplate.delete(u);

		User delUser = cassandraTemplate.selectOne("select * from test.users where username='cassandra';", User.class);

		log.info("delUser => " + delUser);

		Assert.assertNull(delUser);

	}

	// @Test
	public void multiplePKTest() {

		LogEntry l = new LogEntry();
		l.setLogDate(new Date());
		l.setHostname("localhost");
		l.setLogData("Host is Up");

		cassandraTemplate.insert(l);

	}

	// @After
	public void clearCassandra() {
		EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();

	}

	@AfterClass
	public static void stopCassandra() {
		EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
		EmbeddedCassandraServerHelper.stopEmbeddedCassandra();
	}
}
