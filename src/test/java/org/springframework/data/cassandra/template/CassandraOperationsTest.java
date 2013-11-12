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
import java.util.List;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
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
import org.springframework.data.cassandra.test.User;
import org.springframework.data.cassandra.vo.RingMember;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

import com.datastax.driver.core.Session;

/**
 * @author David Webb (dwebb@brightmove.com)
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration (classes = {TestConfig.class}, loader = AnnotationConfigContextLoader.class)
public class CassandraOperationsTest {
	
	@Autowired
	private CassandraTemplate cassandraTemplate;
	
	private static Logger log = LoggerFactory.getLogger(CassandraOperationsTest.class);
	
    protected Session session;
    
    @BeforeClass
    public static void startCassandra()
            throws IOException, TTransportException, ConfigurationException, InterruptedException {
        EmbeddedCassandraServerHelper.startEmbeddedCassandra("cassandra.yaml");
    }
    
    @Before
    public void setupKeyspace() {
    	
    	log.info("Creating Keyspace...");
    	
    	cassandraTemplate.executeQuery("CREATE KEYSPACE test WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");

    	log.info("Using Keyspace...");

    	cassandraTemplate.executeQuery("use test;");

    	log.info("Creating Table...");

    	cassandraTemplate.createTable(User.class);

    	
    }
	
    @Test
    public void ringTest() {
    	
		List<RingMember> ring = cassandraTemplate.describeRing();
		
		/*
		 * There must be 1 node in the cluster if the embedded server is running.
		 */
		assertNotNull(ring);
		
		for (RingMember h: ring) {
			log.info(h.address);
		}
    }
    
    /**
     * This test inserts and selects users from the test.users table
     * This is testing the CassandraTemplate:
     * <ul>
     * <li>insert()</li>
     * <li>selectOne()</li>
     * <li>select()</li>
     * </ul>
     */
    @Test
    public void UsersTest() {
    	
    	User u = new User();
    	u.setUsername("cassandra");
    	u.setFirstName("Apache");
    	u.setLastName("Cassnadra");
    	
    	cassandraTemplate.insert(u, "users");
    	
    	User us = cassandraTemplate.selectOne("select * from test.users where username='cassandra';" , User.class);
    	
    	log.debug("Output from select One");
    	log.debug(us.getFirstName());
    	log.debug(us.getLastName());
    	
    	List<User> users = cassandraTemplate.select("Select * from test.users", User.class);

    	log.debug("Output from select All");
    	for (User x: users) {
        	log.debug(x.getFirstName());
        	log.debug(x.getLastName());
    	}
    	
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
