package org.springframework.data.cassandra.test;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.List;

import lombok.extern.apachecommons.CommonsLog;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.cassandraunit.DataLoader;
import org.cassandraunit.dataset.json.ClassPathJsonDataSet;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.cassandra.core.CassandraTemplate;
import org.springframework.data.cassandra.test.cf.Jobs;
import org.springframework.data.cassandra.test.config.TestConfig;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.TokenRange;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;

/**
 * Bring up the embedded Cassandra server for unit testing
 *
 * @author	David Webb
 */
@CommonsLog
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration (classes = {TestConfig.class}, loader = AnnotationConfigContextLoader.class)
public class BaseCassandraTest {

	@Autowired
	private CassandraTemplate cassandraTemplate;
	
    protected Keyspace keyspace;

    @BeforeClass
    public static void startCassandra()
            throws IOException, TTransportException, ConfigurationException, InterruptedException
    {
        EmbeddedCassandraServerHelper.startEmbeddedCassandra("cassandra.yaml");
    }

    @Before
    public void setUp() throws IOException, TTransportException, ConfigurationException, InterruptedException {
    	
    	log.info("Loading Data ...");
        DataLoader dataLoader = new DataLoader("Test Cluster", "localhost:9160");
        dataLoader.load(new ClassPathJsonDataSet("dataset.json"));

        keyspace = cassandraTemplate.getClient();
        
    }
    
    @Test
    public void baseTest() {
    	
    	log.info("Testing...");
    	
    	assertNotNull(keyspace);
    	
    }
    
    @Test
    public void ringTest() {
    	
    	try {
    		
    		List<TokenRange> ringTokens = keyspace.describeRing();
			
    		/*
    		 * There must be 1 node in the cluster if the embedded server is running.
    		 */
    		assertNotNull(ringTokens);
    		
    		
    		log.info(ringTokens.toString());
			
		} catch (ConnectionException e) {
			e.printStackTrace();
		}

    }
    
    /**
     * Test the findById in the CassandraTemplate
     */
    @Test
    public void findByIdTest() {
    	
    	Jobs found = cassandraTemplate.findById("1", Jobs.class, "Jobs");
    	
    	assertNotNull(found);
    	
    	assertEquals(found.getJobTitle(), "Spring Data Cassandra Developer");
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