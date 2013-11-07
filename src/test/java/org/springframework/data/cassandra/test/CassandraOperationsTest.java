package org.springframework.data.cassandra.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import lombok.extern.log4j.Log4j;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.thrift.TokenRange;
import org.apache.thrift.transport.TTransportException;
import org.cassandraunit.DataLoader;
import org.cassandraunit.dataset.json.ClassPathJsonDataSet;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.cassandra.core.CassandraTemplate;
import org.springframework.data.cassandra.core.bean.RingMember;
import org.springframework.data.cassandra.test.cf.Jobs;

import com.datastax.driver.core.Session;

/**
 * Bring up the embedded Cassandra server for unit testing
 * 
 * Test the Operations Interface
 * 
 * All tests should be created in this abstract class, and it is up to the extending class to define the
 * Spring Configuration for the test cases.
 * 
 * @author	David Webb
 */
@Log4j
public abstract class CassandraOperationsTest {

	@Autowired
	private CassandraTemplate cassandraTemplate;
	
    protected Session session;
    
    private final static String CF_NAME_JOBS = "Jobs";

    @BeforeClass
    public static void startCassandra()
            throws IOException, TTransportException, ConfigurationException, InterruptedException {
        EmbeddedCassandraServerHelper.startEmbeddedCassandra("cassandra.yaml");
    }

    @Before
    public void setUp() throws IOException, TTransportException, ConfigurationException, InterruptedException {
    	
    	log.info("Loading Data ...");
        DataLoader dataLoader = new DataLoader("Test Cluster", "localhost:9160");
        dataLoader.load(new ClassPathJsonDataSet("dataset.json"));

        session = cassandraTemplate.getSession();
        
    }
    
    @Test
    public void baseTest() {
    	
    	log.info("Testing...");
    	
    	assertNotNull(session);
    	
    }
    
    @Test
    public void ringTest() {
    	
		List<RingMember> ring = cassandraTemplate.describeRing();
		
		/*
		 * There must be 1 node in the cluster if the embedded server is running.
		 */
		assertNotNull(ring);
		
		
		log.info(ring.toString());
			
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