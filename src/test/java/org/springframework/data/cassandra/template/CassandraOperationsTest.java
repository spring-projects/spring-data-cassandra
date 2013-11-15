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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

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
import org.springframework.data.cassandra.config.TestConfig;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.core.ConsistencyLevel;
import org.springframework.data.cassandra.core.QueryOptions;
import org.springframework.data.cassandra.core.RetryPolicy;
import org.springframework.data.cassandra.core.RingMember;
import org.springframework.data.cassandra.table.Book;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

/**
 * Unit Tests for CassnadraTemplate
 * 
 * @author David Webb
 * 
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = { TestConfig.class }, loader = AnnotationConfigContextLoader.class)
public class CassandraOperationsTest {

	@Autowired
	private CassandraOperations cassandraTemplate;

	private static Logger log = LoggerFactory.getLogger(CassandraOperationsTest.class);

	private final static String CASSANDRA_CONFIG = "cassandra.yaml";
	private final static String KEYSPACE_NAME = "test";
	private final static String CASSANDRA_HOST = "localhost";
	private final static int CASSANDRA_NATIVE_PORT = 9042;
	private final static int CASSANDRA_THRIFT_PORT = 9160;

	@Rule
	public CassandraCQLUnit cassandraCQLUnit = new CassandraCQLUnit(new ClassPathCQLDataSet("cql-dataload.cql",
			KEYSPACE_NAME), CASSANDRA_CONFIG, CASSANDRA_HOST, CASSANDRA_NATIVE_PORT);

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
			log.info(h.address);
		}
	}

	@Test
	public void insertTest() {

		/*
		 * Test Single Insert with entity
		 */
		Book b1 = new Book();
		b1.setIsbn("123456-1");
		b1.setTitle("Spring Data Cassandra Guide");
		b1.setAuthor("Cassandra Guru");
		b1.setPages(521);

		cassandraTemplate.insert(b1);

		Book b2 = new Book();
		b2.setIsbn("123456-2");
		b2.setTitle("Spring Data Cassandra Guide");
		b2.setAuthor("Cassandra Guru");
		b2.setPages(521);

		cassandraTemplate.insert(b2, "book_alt");

		/*
		 * Test Single Insert with entity
		 */
		Book b3 = new Book();
		b3.setIsbn("123456-3");
		b3.setTitle("Spring Data Cassandra Guide");
		b3.setAuthor("Cassandra Guru");
		b3.setPages(265);

		QueryOptions options = new QueryOptions();
		options.setConsistencyLevel(ConsistencyLevel.ONE);
		options.setRetryPolicy(RetryPolicy.DOWNGRADING_CONSISTENCY);

		cassandraTemplate.insert(b3, "book", options);

		/*
		 * Test Single Insert with entity
		 */
		Book b4 = new Book();
		b4.setIsbn("123456-4");
		b4.setTitle("Spring Data Cassandra Guide");
		b4.setAuthor("Cassandra Guru");
		b4.setPages(465);

		Map<String, Object> optionsByName = new HashMap<String, Object>();
		optionsByName.put(QueryOptions.QueryOptionMapKeys.CONSISTENCY_LEVEL, ConsistencyLevel.ALL);
		optionsByName.put(QueryOptions.QueryOptionMapKeys.RETRY_POLICY, RetryPolicy.FALLTHROUGH);
		optionsByName.put(QueryOptions.QueryOptionMapKeys.TTL, 30);

		cassandraTemplate.insert(b4, "book", optionsByName);

		/*
		 * Test Single Insert with entity
		 */
		Book b5 = new Book();
		b5.setIsbn("123456-5");
		b5.setTitle("Spring Data Cassandra Guide");
		b5.setAuthor("Cassandra Guru");
		b5.setPages(265);

		cassandraTemplate.insert(b5, options);

		/*
		 * Test Single Insert with entity
		 */
		Book b6 = new Book();
		b6.setIsbn("123456-6");
		b6.setTitle("Spring Data Cassandra Guide");
		b6.setAuthor("Cassandra Guru");
		b6.setPages(465);

		cassandraTemplate.insert(b6, optionsByName);

	}

	@Test
	public void insertAsynchronouslyTest() {

		/*
		 * Test Single Insert with entity
		 */
		Book b1 = new Book();
		b1.setIsbn("123456-1");
		b1.setTitle("Spring Data Cassandra Guide");
		b1.setAuthor("Cassandra Guru");
		b1.setPages(521);

		cassandraTemplate.insertAsynchronously(b1);

		Book b2 = new Book();
		b2.setIsbn("123456-2");
		b2.setTitle("Spring Data Cassandra Guide");
		b2.setAuthor("Cassandra Guru");
		b2.setPages(521);

		cassandraTemplate.insertAsynchronously(b2, "book_alt");

		/*
		 * Test Single Insert with entity
		 */
		Book b3 = new Book();
		b3.setIsbn("123456-3");
		b3.setTitle("Spring Data Cassandra Guide");
		b3.setAuthor("Cassandra Guru");
		b3.setPages(265);

		QueryOptions options = new QueryOptions();
		options.setConsistencyLevel(ConsistencyLevel.ONE);
		options.setRetryPolicy(RetryPolicy.DOWNGRADING_CONSISTENCY);

		cassandraTemplate.insertAsynchronously(b3, "book", options);

		/*
		 * Test Single Insert with entity
		 */
		Book b4 = new Book();
		b4.setIsbn("123456-4");
		b4.setTitle("Spring Data Cassandra Guide");
		b4.setAuthor("Cassandra Guru");
		b4.setPages(465);

		Map<String, Object> optionsByName = new HashMap<String, Object>();
		optionsByName.put(QueryOptions.QueryOptionMapKeys.CONSISTENCY_LEVEL, ConsistencyLevel.ALL);
		optionsByName.put(QueryOptions.QueryOptionMapKeys.RETRY_POLICY, RetryPolicy.FALLTHROUGH);
		optionsByName.put(QueryOptions.QueryOptionMapKeys.TTL, 30);

		cassandraTemplate.insertAsynchronously(b4, "book", optionsByName);

		/*
		 * Test Single Insert with entity
		 */
		Book b5 = new Book();
		b5.setIsbn("123456-5");
		b5.setTitle("Spring Data Cassandra Guide");
		b5.setAuthor("Cassandra Guru");
		b5.setPages(265);

		cassandraTemplate.insertAsynchronously(b5, options);

		/*
		 * Test Single Insert with entity
		 */
		Book b6 = new Book();
		b6.setIsbn("123456-6");
		b6.setTitle("Spring Data Cassandra Guide");
		b6.setAuthor("Cassandra Guru");
		b6.setPages(465);

		cassandraTemplate.insertAsynchronously(b6, optionsByName);

	}

	@Test
	public void insertBatchTest() {

		QueryOptions options = new QueryOptions();
		options.setConsistencyLevel(ConsistencyLevel.ONE);
		options.setRetryPolicy(RetryPolicy.DOWNGRADING_CONSISTENCY);

		Map<String, Object> optionsByName = new HashMap<String, Object>();
		optionsByName.put(QueryOptions.QueryOptionMapKeys.CONSISTENCY_LEVEL, ConsistencyLevel.ALL);
		optionsByName.put(QueryOptions.QueryOptionMapKeys.RETRY_POLICY, RetryPolicy.FALLTHROUGH);
		optionsByName.put(QueryOptions.QueryOptionMapKeys.TTL, 30);

		List<Book> books = null;

		books = getBookList(20);

		cassandraTemplate.insert(books);

		books = getBookList(20);

		cassandraTemplate.insert(books, "book_alt");

		books = getBookList(20);

		cassandraTemplate.insert(books, "book", options);

		books = getBookList(20);

		cassandraTemplate.insert(books, "book", optionsByName);

		books = getBookList(20);

		cassandraTemplate.insert(books, options);

		books = getBookList(20);

		cassandraTemplate.insert(books, optionsByName);

	}

	@Test
	public void insertBatchAsynchronouslyTest() {

		QueryOptions options = new QueryOptions();
		options.setConsistencyLevel(ConsistencyLevel.ONE);
		options.setRetryPolicy(RetryPolicy.DOWNGRADING_CONSISTENCY);

		Map<String, Object> optionsByName = new HashMap<String, Object>();
		optionsByName.put(QueryOptions.QueryOptionMapKeys.CONSISTENCY_LEVEL, ConsistencyLevel.ALL);
		optionsByName.put(QueryOptions.QueryOptionMapKeys.RETRY_POLICY, RetryPolicy.FALLTHROUGH);
		optionsByName.put(QueryOptions.QueryOptionMapKeys.TTL, 30);

		List<Book> books = null;

		books = getBookList(20);

		cassandraTemplate.insertAsynchronously(books);

		books = getBookList(20);

		cassandraTemplate.insertAsynchronously(books, "book_alt");

		books = getBookList(20);

		cassandraTemplate.insertAsynchronously(books, "book", options);

		books = getBookList(20);

		cassandraTemplate.insertAsynchronously(books, "book", optionsByName);

		books = getBookList(20);

		cassandraTemplate.insertAsynchronously(books, options);

		books = getBookList(20);

		cassandraTemplate.insertAsynchronously(books, optionsByName);

	}

	/**
	 * @return
	 */
	private List<Book> getBookList(int numBooks) {

		List<Book> books = new ArrayList<Book>();

		Book b = null;
		for (int i = 0; i < numBooks; i++) {
			b = new Book();
			b.setIsbn(UUID.randomUUID().toString());
			b.setTitle("Spring Data Cassandra Guide");
			b.setAuthor("Cassandra Guru");
			b.setPages(i * 10 + 5);
			books.add(b);
		}

		return books;
	}

	@Test
	public void updateTest() {

		insertTest();

		QueryOptions options = new QueryOptions();
		options.setConsistencyLevel(ConsistencyLevel.ONE);
		options.setRetryPolicy(RetryPolicy.DOWNGRADING_CONSISTENCY);

		Map<String, Object> optionsByName = new HashMap<String, Object>();
		optionsByName.put(QueryOptions.QueryOptionMapKeys.CONSISTENCY_LEVEL, ConsistencyLevel.ALL);
		optionsByName.put(QueryOptions.QueryOptionMapKeys.RETRY_POLICY, RetryPolicy.FALLTHROUGH);
		optionsByName.put(QueryOptions.QueryOptionMapKeys.TTL, 30);

		/*
		 * Test Single Insert with entity
		 */
		Book b1 = new Book();
		b1.setIsbn("123456-1");
		b1.setTitle("Spring Data Cassandra Book");
		b1.setAuthor("Cassandra Guru");
		b1.setPages(521);

		cassandraTemplate.update(b1);

		Book b2 = new Book();
		b2.setIsbn("123456-2");
		b2.setTitle("Spring Data Cassandra Book");
		b2.setAuthor("Cassandra Guru");
		b2.setPages(521);

		cassandraTemplate.update(b2, "book_alt");

		/*
		 * Test Single Insert with entity
		 */
		Book b3 = new Book();
		b3.setIsbn("123456-3");
		b3.setTitle("Spring Data Cassandra Book");
		b3.setAuthor("Cassandra Guru");
		b3.setPages(265);

		cassandraTemplate.update(b3, "book", options);

		/*
		 * Test Single Insert with entity
		 */
		Book b4 = new Book();
		b4.setIsbn("123456-4");
		b4.setTitle("Spring Data Cassandra Book");
		b4.setAuthor("Cassandra Guru");
		b4.setPages(465);

		cassandraTemplate.update(b4, "book", optionsByName);

		/*
		 * Test Single Insert with entity
		 */
		Book b5 = new Book();
		b5.setIsbn("123456-5");
		b5.setTitle("Spring Data Cassandra Book");
		b5.setAuthor("Cassandra Guru");
		b5.setPages(265);

		cassandraTemplate.update(b5, options);

		/*
		 * Test Single Insert with entity
		 */
		Book b6 = new Book();
		b6.setIsbn("123456-6");
		b6.setTitle("Spring Data Cassandra Book");
		b6.setAuthor("Cassandra Guru");
		b6.setPages(465);

		cassandraTemplate.update(b6, optionsByName);

	}

	@Test
	public void updateAsynchronouslyTest() {

		insertTest();

		QueryOptions options = new QueryOptions();
		options.setConsistencyLevel(ConsistencyLevel.ONE);
		options.setRetryPolicy(RetryPolicy.DOWNGRADING_CONSISTENCY);

		Map<String, Object> optionsByName = new HashMap<String, Object>();
		optionsByName.put(QueryOptions.QueryOptionMapKeys.CONSISTENCY_LEVEL, ConsistencyLevel.ALL);
		optionsByName.put(QueryOptions.QueryOptionMapKeys.RETRY_POLICY, RetryPolicy.FALLTHROUGH);
		optionsByName.put(QueryOptions.QueryOptionMapKeys.TTL, 30);

		/*
		 * Test Single Insert with entity
		 */
		Book b1 = new Book();
		b1.setIsbn("123456-1");
		b1.setTitle("Spring Data Cassandra Book");
		b1.setAuthor("Cassandra Guru");
		b1.setPages(521);

		cassandraTemplate.updateAsynchronously(b1);

		Book b2 = new Book();
		b2.setIsbn("123456-2");
		b2.setTitle("Spring Data Cassandra Book");
		b2.setAuthor("Cassandra Guru");
		b2.setPages(521);

		cassandraTemplate.updateAsynchronously(b2, "book_alt");

		/*
		 * Test Single Insert with entity
		 */
		Book b3 = new Book();
		b3.setIsbn("123456-3");
		b3.setTitle("Spring Data Cassandra Book");
		b3.setAuthor("Cassandra Guru");
		b3.setPages(265);

		cassandraTemplate.updateAsynchronously(b3, "book", options);

		/*
		 * Test Single Insert with entity
		 */
		Book b4 = new Book();
		b4.setIsbn("123456-4");
		b4.setTitle("Spring Data Cassandra Book");
		b4.setAuthor("Cassandra Guru");
		b4.setPages(465);

		cassandraTemplate.updateAsynchronously(b4, "book", optionsByName);

		/*
		 * Test Single Insert with entity
		 */
		Book b5 = new Book();
		b5.setIsbn("123456-5");
		b5.setTitle("Spring Data Cassandra Book");
		b5.setAuthor("Cassandra Guru");
		b5.setPages(265);

		cassandraTemplate.updateAsynchronously(b5, options);

		/*
		 * Test Single Insert with entity
		 */
		Book b6 = new Book();
		b6.setIsbn("123456-6");
		b6.setTitle("Spring Data Cassandra Book");
		b6.setAuthor("Cassandra Guru");
		b6.setPages(465);

		cassandraTemplate.updateAsynchronously(b6, optionsByName);

	}

	@Test
	public void deleteTest() {

		insertTest();

		QueryOptions options = new QueryOptions();
		options.setConsistencyLevel(ConsistencyLevel.ONE);
		options.setRetryPolicy(RetryPolicy.DOWNGRADING_CONSISTENCY);

		Map<String, Object> optionsByName = new HashMap<String, Object>();
		optionsByName.put(QueryOptions.QueryOptionMapKeys.CONSISTENCY_LEVEL, ConsistencyLevel.ALL);
		optionsByName.put(QueryOptions.QueryOptionMapKeys.RETRY_POLICY, RetryPolicy.FALLTHROUGH);

		/*
		 * Test Single Insert with entity
		 */
		Book b1 = new Book();
		b1.setIsbn("123456-1");

		cassandraTemplate.delete(b1);

		Book b2 = new Book();
		b2.setIsbn("123456-2");

		cassandraTemplate.delete(b2, "book_alt");

		/*
		 * Test Single Insert with entity
		 */
		Book b3 = new Book();
		b3.setIsbn("123456-3");

		cassandraTemplate.delete(b3, "book", options);

		/*
		 * Test Single Insert with entity
		 */
		Book b4 = new Book();
		b4.setIsbn("123456-4");

		cassandraTemplate.delete(b4, "book", optionsByName);

		/*
		 * Test Single Insert with entity
		 */
		Book b5 = new Book();
		b5.setIsbn("123456-5");

		cassandraTemplate.delete(b5, options);

		/*
		 * Test Single Insert with entity
		 */
		Book b6 = new Book();
		b6.setIsbn("123456-6");

		cassandraTemplate.delete(b6, optionsByName);

	}

	@Test
	public void deleteAsynchronouslyTest() {

		insertTest();

		QueryOptions options = new QueryOptions();
		options.setConsistencyLevel(ConsistencyLevel.ONE);
		options.setRetryPolicy(RetryPolicy.DOWNGRADING_CONSISTENCY);

		Map<String, Object> optionsByName = new HashMap<String, Object>();
		optionsByName.put(QueryOptions.QueryOptionMapKeys.CONSISTENCY_LEVEL, ConsistencyLevel.ALL);
		optionsByName.put(QueryOptions.QueryOptionMapKeys.RETRY_POLICY, RetryPolicy.FALLTHROUGH);

		/*
		 * Test Single Insert with entity
		 */
		Book b1 = new Book();
		b1.setIsbn("123456-1");

		cassandraTemplate.deleteAsynchronously(b1);

		Book b2 = new Book();
		b2.setIsbn("123456-2");

		cassandraTemplate.deleteAsynchronously(b2, "book_alt");

		/*
		 * Test Single Insert with entity
		 */
		Book b3 = new Book();
		b3.setIsbn("123456-3");

		cassandraTemplate.deleteAsynchronously(b3, "book", options);

		/*
		 * Test Single Insert with entity
		 */
		Book b4 = new Book();
		b4.setIsbn("123456-4");

		cassandraTemplate.deleteAsynchronously(b4, "book", optionsByName);

		/*
		 * Test Single Insert with entity
		 */
		Book b5 = new Book();
		b5.setIsbn("123456-5");

		cassandraTemplate.deleteAsynchronously(b5, options);

		/*
		 * Test Single Insert with entity
		 */
		Book b6 = new Book();
		b6.setIsbn("123456-6");

		cassandraTemplate.deleteAsynchronously(b6, optionsByName);
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
