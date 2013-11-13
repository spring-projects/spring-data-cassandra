/**
 * All BrightMove Code is Copyright 2004-2013 BrightMove Inc.
 * Modification of code without the express written consent of
 * BrightMove, Inc. is strictly forbidden.
 *
 * Author: David Webb (dwebb@brightmove.com)
 * Created On: Nov 11, 2013 
 */
package org.springframework.data.cassandra.template;

import java.io.IOException;

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
import org.mockito.Mock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.data.cassandra.config.TestConfig;
import org.springframework.data.cassandra.core.CassandraTemplate;
import org.springframework.data.cassandra.test.Comment;
import org.springframework.data.cassandra.test.User;
import org.springframework.data.cassandra.test.UserAlter;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

/**
 * @author David Webb (dwebb@brightmove.com)
 * 
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = { TestConfig.class }, loader = AnnotationConfigContextLoader.class)
public class CassandraOperationsTableTest {

	@Autowired
	private CassandraTemplate cassandraTemplate;

	@Mock
	ApplicationContext context;

	private static Logger log = LoggerFactory.getLogger(CassandraOperationsTableTest.class);

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

		cassandraTemplate.createTable(User.class);
		cassandraTemplate.createTable(Comment.class);

	}

	@Test
	public void alterTableTest() {

		cassandraTemplate.alterTable(UserAlter.class);

	}

	@Test
	public void dropTableTest() {

		cassandraTemplate.dropTable(User.class);
		cassandraTemplate.dropTable("comments");

	}

	@After
	public void clearCassandra() {
		EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
	}

	@AfterClass
	public static void stopCassandra() {
		EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
	}
}
