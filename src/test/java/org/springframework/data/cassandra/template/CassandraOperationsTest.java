/**
 * All BrightMove Code is Copyright 2004-2013 BrightMove Inc.
 * Modification of code without the express written consent of
 * BrightMove, Inc. is strictly forbidden.
 *
 * Author: David Webb (dwebb@brightmove.com)
 * Created On: Nov 11, 2013 
 */
package org.springframework.data.cassandra.template;

import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.Date;
import java.util.List;

import junit.framework.Assert;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.cassandraunit.DataLoader;
import org.cassandraunit.dataset.yaml.ClassPathYamlDataSet;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.cassandra.config.TestConfig;
import org.springframework.data.cassandra.core.CassandraTemplate;
import org.springframework.data.cassandra.core.RingMember;
import org.springframework.data.cassandra.test.LogEntry;
import org.springframework.data.cassandra.test.User;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

import com.datastax.driver.core.Session;

/**
 * @author David Webb (dwebb@brightmove.com)
 * 
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = { TestConfig.class }, loader = AnnotationConfigContextLoader.class)
public class CassandraOperationsTest {

	@Autowired
	private CassandraTemplate cassandraTemplate;

	private static Logger log = LoggerFactory.getLogger(CassandraOperationsTest.class);

	protected Session session;

	@BeforeClass
	public static void startCassandra() throws IOException, TTransportException, ConfigurationException,
			InterruptedException {
		EmbeddedCassandraServerHelper.startEmbeddedCassandra("cassandra.yaml");

		/*
		 * Load data file to creat the test keyspace before we init the template
		 */
		DataLoader dataLoader = new DataLoader("Test Cluster", "localhost:9160");
		dataLoader.load(new ClassPathYamlDataSet("cassandra-data.yaml"));

	}

	@Before
	public void setupKeyspace() {

		/*
		 * Load data file to creat the test keyspace before we init the template
		 */
		DataLoader dataLoader = new DataLoader("Test Cluster", "localhost:9160");
		dataLoader.load(new ClassPathYamlDataSet("cassandra-data.yaml"));

		log.info("Creating Table...");

		// cassandraTemplate.createTable(User.class);

		// cassandraTemplate.createTable(LogEntry.class);

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

	/**
	 * This test inserts and selects users from the test.users table This is testing the CassandraTemplate:
	 * <ul>
	 * <li>insert()</li>
	 * <li>selectOne()</li>
	 * <li>select()</li>
	 * <li>remove()</li>
	 * </ul>
	 */
	// @Test
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

	@After
	public void clearCassandra() {
		EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();

	}

	@AfterClass
	public static void stopCassandra() {
		EmbeddedCassandraServerHelper.stopEmbeddedCassandra();
	}
}
