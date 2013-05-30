package org.springframework.data.cassandra.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import lombok.extern.log4j.Log4j;

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
 * Test the Operations Interface
 *
 * @author	David Webb
 */
@Log4j
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration (classes = {TestConfig.class}, loader = AnnotationConfigContextLoader.class)
public class CassandraOperationsTest {

	@Autowired
	private CassandraTemplate cassandraTemplate;
	
    protected Keyspace keyspace;
    
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
    	
    	Jobs found = cassandraTemplate.findById("1", Jobs.class, CF_NAME_JOBS);
    	
    	assertNotNull(found);
    	
    	assertEquals(found.getJobTitle(), "Spring Data Cassandra Developer");
    }
    
    /**
     * Test the findAll in the CassandraTemplate
     */
    @Test
    public void findAllTest() {
    	
    	List<Jobs> found = cassandraTemplate.findAll(Jobs.class, CF_NAME_JOBS);
    	
    	assertNotNull(found);
    	
    	assertEquals(found.size(), 3);
    }
    
    @Test
    public void insertOneRemoveOne() {
    	
    	String testKey = "TEST-904";
    	String testTitle = "TEST-904-Title";
    	String testPayRate = "75.00";
    	
    	Jobs job1 = new Jobs();
    	job1.setKey(testKey);
    	job1.setJobTitle(testTitle);
    	job1.setPayRate(testPayRate);
    	
    	cassandraTemplate.insert(job1, Jobs.class, CF_NAME_JOBS);
    	
    	Jobs jobGet = cassandraTemplate.findById(testKey, Jobs.class, CF_NAME_JOBS);
    	
    	assertNotNull(jobGet);
    	assertEquals(job1.getKey(), jobGet.getKey());
    	assertEquals(job1.getJobTitle(), jobGet.getJobTitle());
    	assertEquals(job1.getPayRate(), jobGet.getPayRate());
    	
    	cassandraTemplate.remove(jobGet, Jobs.class, CF_NAME_JOBS);
    	
    	jobGet = cassandraTemplate.findById(testKey, Jobs.class, CF_NAME_JOBS);
    	
    	assertNull(jobGet);

    }
    
    @Test
    public void insertCollectionRemoveCollection() {
    	
    	String testKey1 = "TEST-904";
    	String testTitle1 = "TEST-904-Title";
    	String testPayRate1 = "75.00";
    	
    	Jobs job1 = new Jobs();
    	job1.setKey(testKey1);
    	job1.setJobTitle(testTitle1);
    	job1.setPayRate(testPayRate1);
    	
    	String testKey2 = "TEST-9042";
    	String testTitle2 = "TEST-9042-Title";
    	String testPayRate2 = "76.00";
    	
    	Jobs job2 = new Jobs();
    	job2.setKey(testKey2);
    	job2.setJobTitle(testTitle2);
    	job2.setPayRate(testPayRate2);
    	
    	List<Jobs> list = new ArrayList<Jobs>();
    	list.add(job1);
    	list.add(job2);
    	
    	cassandraTemplate.insert(list, Jobs.class, CF_NAME_JOBS);
    	
    	Jobs jobGet1 = cassandraTemplate.findById(testKey1, Jobs.class, CF_NAME_JOBS);
    	
    	assertNotNull(jobGet1);
    	assertEquals(job1.getKey(), jobGet1.getKey());
    	assertEquals(job1.getJobTitle(), jobGet1.getJobTitle());
    	assertEquals(job1.getPayRate(), jobGet1.getPayRate());
    	
    	Jobs jobGet2 = cassandraTemplate.findById(testKey2, Jobs.class, CF_NAME_JOBS);
    	
    	assertNotNull(jobGet2);
    	assertEquals(job2.getKey(), jobGet2.getKey());
    	assertEquals(job2.getJobTitle(), jobGet2.getJobTitle());
    	assertEquals(job2.getPayRate(), jobGet2.getPayRate());
    	
    	cassandraTemplate.remove(list, Jobs.class, CF_NAME_JOBS);
    	
    	jobGet1 = cassandraTemplate.findById(testKey1, Jobs.class, CF_NAME_JOBS);
    	
    	assertNull(jobGet1);

    	jobGet2 = cassandraTemplate.findById(testKey2, Jobs.class, CF_NAME_JOBS);
    	
    	assertNull(jobGet2);

    }
    
    @Test
    public void saveOne() {
    	
    	final String testJobID = "1";
    	final String seedJobTitle = "Spring Data Cassandra Developer";
    	final String newJobTitle = "Struts 1.1 Developer";
    	
    	Jobs job = cassandraTemplate.findById(testJobID, Jobs.class, CF_NAME_JOBS);
    	
    	assertNotNull(job);
    	
    	assertEquals(job.getJobTitle(), seedJobTitle);
    	
    	job.setJobTitle(newJobTitle);
    	
    	cassandraTemplate.save(job, Jobs.class, CF_NAME_JOBS);
    	
    	job = cassandraTemplate.findById(testJobID, Jobs.class, CF_NAME_JOBS);

    	assertNotNull(job);
    	
    	assertEquals(job.getJobTitle(), newJobTitle);
    }
    	
    @Test
    public void saveCollection() {
    	
    	final String testJobID1 = "1";
    	final String seedJobTitle1 = "Spring Data Cassandra Developer";
    	final String newJobTitle1 = "Struts 1.1 Developer";
    	
    	final String testJobID2 = "2";
    	final String seedJobTitle2 = "Spring Data Cassandra User";
    	final String newJobTitle2 = "Struts User";
    	
    	Jobs job1 = cassandraTemplate.findById(testJobID1, Jobs.class, CF_NAME_JOBS);
    	
    	assertNotNull(job1);
    	
    	assertEquals(job1.getJobTitle(), seedJobTitle1);
    	
    	Jobs job2 = cassandraTemplate.findById(testJobID2, Jobs.class, CF_NAME_JOBS);
    	
    	assertNotNull(job2);
    	
    	assertEquals(job2.getJobTitle(), seedJobTitle2);

    	job1.setJobTitle(newJobTitle1);
    	job2.setJobTitle(newJobTitle2);
    	
    	List<Jobs> list = new ArrayList<Jobs>();
    	list.add(job1);
    	list.add(job2);
    	
    	cassandraTemplate.save(list, Jobs.class, CF_NAME_JOBS);
    	
    	job1 = cassandraTemplate.findById(testJobID1, Jobs.class, CF_NAME_JOBS);

    	assertNotNull(job1);
    	
    	assertEquals(job1.getJobTitle(), newJobTitle1);
    	
    	job2 = cassandraTemplate.findById(testJobID2, Jobs.class, CF_NAME_JOBS);

    	assertNotNull(job2);
    	
    	assertEquals(job2.getJobTitle(), newJobTitle2);
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