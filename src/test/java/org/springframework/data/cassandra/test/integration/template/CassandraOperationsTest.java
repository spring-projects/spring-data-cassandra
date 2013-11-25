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
package org.springframework.data.cassandra.test.integration.template;

import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import junit.framework.Assert;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.cassandraunit.CassandraCQLUnit;
import org.cassandraunit.DataLoader;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.cassandraunit.dataset.yaml.ClassPathYamlDataSet;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cassandra.core.BoundStatementFactory;
import org.springframework.cassandra.core.CassandraOperations;
import org.springframework.cassandra.core.CqlParameter;
import org.springframework.cassandra.core.CqlParameterValue;
import org.springframework.cassandra.core.HostMapper;
import org.springframework.cassandra.core.PreparedStatementCreatorFactory;
import org.springframework.cassandra.core.ResultSetExtractor;
import org.springframework.cassandra.core.RingMember;
import org.springframework.dao.DataAccessException;
import org.springframework.data.cassandra.test.integration.config.TestConfig;
import org.springframework.data.cassandra.test.integration.table.Book;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.exceptions.DriverException;

/**
 * Unit Tests for CassandraTemplate
 * 
 * @author David Webb
 * 
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = { TestConfig.class }, loader = AnnotationConfigContextLoader.class)
public class CassandraOperationsTest {

	/**
	 * @author David Webb
	 * 
	 */
	public class MyHost {

		public String someName;

	}

	@Autowired
	private CassandraOperations cassandraTemplate;

	private static Logger log = LoggerFactory.getLogger(CassandraOperationsTest.class);

	private final static String CASSANDRA_CONFIG = "cassandra.yaml";
	private final static String KEYSPACE_NAME = "test";
	private final static String CASSANDRA_HOST = "localhost";
	private final static int CASSANDRA_NATIVE_PORT = 9042;
	private final static int CASSANDRA_THRIFT_PORT = 9160;

	@Rule
	public CassandraCQLUnit cassandraCQLUnit = new CassandraCQLUnit(new ClassPathCQLDataSet(
			"cassandraOperationsTest-cql-dataload.cql", KEYSPACE_NAME), CASSANDRA_CONFIG, CASSANDRA_HOST,
			CASSANDRA_NATIVE_PORT);

	@BeforeClass
	public static void startCassandra() throws IOException, TTransportException, ConfigurationException,
			InterruptedException {

		EmbeddedCassandraServerHelper.startEmbeddedCassandra(CASSANDRA_CONFIG);

		/*
		 * Load data file to creat the test keyspace before we init the template
		 */
		DataLoader dataLoader = new DataLoader("Test Cluster", CASSANDRA_HOST + ":" + CASSANDRA_THRIFT_PORT);
		dataLoader.load(new ClassPathYamlDataSet("cassandra-keyspace.yaml"));
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
			log.info("ringTest Host -> " + h.address);
		}
	}

	@Test
	public void hostMapperTest() {

		List<MyHost> ring = (List<MyHost>) cassandraTemplate.describeRing(new HostMapper<MyHost>() {

			@Override
			public Collection<MyHost> mapHosts(Set<Host> host) throws DriverException {

				List<MyHost> list = new LinkedList<CassandraOperationsTest.MyHost>();

				for (Host h : host) {
					MyHost mh = new MyHost();
					mh.someName = h.getAddress().getCanonicalHostName();
					list.add(mh);
				}

				return list;
			}

		});

		assertNotNull(ring);
		Assert.assertTrue(ring.size() > 0);

		for (MyHost h : ring) {
			log.info("hostMapperTest Host -> " + h.someName);
		}

	}

	@Test
	public void preparedStatementFactoryTest() {

		String cql = "select * from book where isbn = ?";

		List<CqlParameter> parameters = new LinkedList<CqlParameter>();
		parameters.add(new CqlParameter("isbn", DataType.text()));

		PreparedStatementCreatorFactory factory = new PreparedStatementCreatorFactory(cql, parameters);

		List<CqlParameterValue> values = new LinkedList<CqlParameterValue>();
		values.add(new CqlParameterValue(DataType.text(), "999999999"));

		Book b = cassandraTemplate.query(factory.newPreparedStatementCreator(values),
				factory.newPreparedStatementBinder(values), new ResultSetExtractor<Book>() {

					@Override
					public Book extractData(ResultSet rs) throws DriverException, DataAccessException {
						Row r = rs.one();
						Book b = new Book();
						b.setIsbn(r.getString("isbn"));
						b.setTitle(r.getString("title"));
						b.setAuthor(r.getString("author"));
						b.setPages(r.getInt("pages"));
						return b;
					}
				});

		log.info(b.toString());

	}

	@Test
	public void boundStatementFactoryTest() {

		String cql = "insert into book (isbn, title, author, pages) values (?, ?, ?, ?)";

		BoundStatementFactory bsf = new BoundStatementFactory(cql);
		bsf.addValues(new Object[] { "1234", "Moby Dick", "Herman Manville", new Integer(456) }, new Object[] { "2345",
				"War and Peace", "Russian Dude", new Integer(456) }, new Object[] { "3456", "Jane Ayre", "Charlotte",
				new Integer(456) });

		cassandraTemplate.execute(bsf);
	}

	@After
	public void clearCassandra() {
		EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();

	}

	@AfterClass
	public static void stopCassandra() {
		// EmbeddedCassandraServerHelper.stopEmbeddedCassandra();
	}
}
