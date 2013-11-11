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
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.cassandra.config.TestConfig;
import org.springframework.data.cassandra.core.CassandraTemplate;
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
    
    @After
    public void clearCassandra() {
        EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
    }

    @AfterClass
    public static void stopCassandra() {
    	EmbeddedCassandraServerHelper.stopEmbeddedCassandra();
    }
}
