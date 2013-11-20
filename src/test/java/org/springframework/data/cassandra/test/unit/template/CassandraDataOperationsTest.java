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
package org.springframework.data.cassandra.test.unit.template;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

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
import org.springframework.data.cassandra.core.CassandraDataOperations;
import org.springframework.data.cassandra.core.ConsistencyLevel;
import org.springframework.data.cassandra.core.QueryOptions;
import org.springframework.data.cassandra.core.RetryPolicy;
import org.springframework.data.cassandra.test.unit.config.TestConfig;
import org.springframework.data.cassandra.test.unit.table.Book;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;

/**
 * Unit Tests for CassandraTemplate
 * 
 * @author David Webb
 * 
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = { TestConfig.class }, loader = AnnotationConfigContextLoader.class)
public class CassandraDataOperationsTest {

	@Autowired
	private CassandraDataOperations cassandraDataTemplate;

	private static Logger log = LoggerFactory.getLogger(CassandraDataOperationsTest.class);

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
	public void insertTest() {

		/*
		 * Test Single Insert with entity
		 */
		Book b1 = new Book();
		b1.setIsbn("123456-1");
		b1.setTitle("Spring Data Cassandra Guide");
		b1.setAuthor("Cassandra Guru");
		b1.setPages(521);

		cassandraDataTemplate.insert(b1);

		Book b2 = new Book();
		b2.setIsbn("123456-2");
		b2.setTitle("Spring Data Cassandra Guide");
		b2.setAuthor("Cassandra Guru");
		b2.setPages(521);

		cassandraDataTemplate.insert(b2, "book_alt");

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

		cassandraDataTemplate.insert(b3, "book", options);

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

		cassandraDataTemplate.insert(b4, "book", optionsByName);

		/*
		 * Test Single Insert with entity
		 */
		Book b5 = new Book();
		b5.setIsbn("123456-5");
		b5.setTitle("Spring Data Cassandra Guide");
		b5.setAuthor("Cassandra Guru");
		b5.setPages(265);

		cassandraDataTemplate.insert(b5, options);

		/*
		 * Test Single Insert with entity
		 */
		Book b6 = new Book();
		b6.setIsbn("123456-6");
		b6.setTitle("Spring Data Cassandra Guide");
		b6.setAuthor("Cassandra Guru");
		b6.setPages(465);

		cassandraDataTemplate.insert(b6, optionsByName);

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

		cassandraDataTemplate.insertAsynchronously(b1);

		Book b2 = new Book();
		b2.setIsbn("123456-2");
		b2.setTitle("Spring Data Cassandra Guide");
		b2.setAuthor("Cassandra Guru");
		b2.setPages(521);

		cassandraDataTemplate.insertAsynchronously(b2, "book_alt");

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

		cassandraDataTemplate.insertAsynchronously(b3, "book", options);

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

		cassandraDataTemplate.insertAsynchronously(b4, "book", optionsByName);

		/*
		 * Test Single Insert with entity
		 */
		Book b5 = new Book();
		b5.setIsbn("123456-5");
		b5.setTitle("Spring Data Cassandra Guide");
		b5.setAuthor("Cassandra Guru");
		b5.setPages(265);

		cassandraDataTemplate.insertAsynchronously(b5, options);

		/*
		 * Test Single Insert with entity
		 */
		Book b6 = new Book();
		b6.setIsbn("123456-6");
		b6.setTitle("Spring Data Cassandra Guide");
		b6.setAuthor("Cassandra Guru");
		b6.setPages(465);

		cassandraDataTemplate.insertAsynchronously(b6, optionsByName);

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

		cassandraDataTemplate.insert(books);

		books = getBookList(20);

		cassandraDataTemplate.insert(books, "book_alt");

		books = getBookList(20);

		cassandraDataTemplate.insert(books, "book", options);

		books = getBookList(20);

		cassandraDataTemplate.insert(books, "book", optionsByName);

		books = getBookList(20);

		cassandraDataTemplate.insert(books, options);

		books = getBookList(20);

		cassandraDataTemplate.insert(books, optionsByName);

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

		cassandraDataTemplate.insertAsynchronously(books);

		books = getBookList(20);

		cassandraDataTemplate.insertAsynchronously(books, "book_alt");

		books = getBookList(20);

		cassandraDataTemplate.insertAsynchronously(books, "book", options);

		books = getBookList(20);

		cassandraDataTemplate.insertAsynchronously(books, "book", optionsByName);

		books = getBookList(20);

		cassandraDataTemplate.insertAsynchronously(books, options);

		books = getBookList(20);

		cassandraDataTemplate.insertAsynchronously(books, optionsByName);

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

		cassandraDataTemplate.update(b1);

		Book b2 = new Book();
		b2.setIsbn("123456-2");
		b2.setTitle("Spring Data Cassandra Book");
		b2.setAuthor("Cassandra Guru");
		b2.setPages(521);

		cassandraDataTemplate.update(b2, "book_alt");

		/*
		 * Test Single Insert with entity
		 */
		Book b3 = new Book();
		b3.setIsbn("123456-3");
		b3.setTitle("Spring Data Cassandra Book");
		b3.setAuthor("Cassandra Guru");
		b3.setPages(265);

		cassandraDataTemplate.update(b3, "book", options);

		/*
		 * Test Single Insert with entity
		 */
		Book b4 = new Book();
		b4.setIsbn("123456-4");
		b4.setTitle("Spring Data Cassandra Book");
		b4.setAuthor("Cassandra Guru");
		b4.setPages(465);

		cassandraDataTemplate.update(b4, "book", optionsByName);

		/*
		 * Test Single Insert with entity
		 */
		Book b5 = new Book();
		b5.setIsbn("123456-5");
		b5.setTitle("Spring Data Cassandra Book");
		b5.setAuthor("Cassandra Guru");
		b5.setPages(265);

		cassandraDataTemplate.update(b5, options);

		/*
		 * Test Single Insert with entity
		 */
		Book b6 = new Book();
		b6.setIsbn("123456-6");
		b6.setTitle("Spring Data Cassandra Book");
		b6.setAuthor("Cassandra Guru");
		b6.setPages(465);

		cassandraDataTemplate.update(b6, optionsByName);

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

		cassandraDataTemplate.updateAsynchronously(b1);

		Book b2 = new Book();
		b2.setIsbn("123456-2");
		b2.setTitle("Spring Data Cassandra Book");
		b2.setAuthor("Cassandra Guru");
		b2.setPages(521);

		cassandraDataTemplate.updateAsynchronously(b2, "book_alt");

		/*
		 * Test Single Insert with entity
		 */
		Book b3 = new Book();
		b3.setIsbn("123456-3");
		b3.setTitle("Spring Data Cassandra Book");
		b3.setAuthor("Cassandra Guru");
		b3.setPages(265);

		cassandraDataTemplate.updateAsynchronously(b3, "book", options);

		/*
		 * Test Single Insert with entity
		 */
		Book b4 = new Book();
		b4.setIsbn("123456-4");
		b4.setTitle("Spring Data Cassandra Book");
		b4.setAuthor("Cassandra Guru");
		b4.setPages(465);

		cassandraDataTemplate.updateAsynchronously(b4, "book", optionsByName);

		/*
		 * Test Single Insert with entity
		 */
		Book b5 = new Book();
		b5.setIsbn("123456-5");
		b5.setTitle("Spring Data Cassandra Book");
		b5.setAuthor("Cassandra Guru");
		b5.setPages(265);

		cassandraDataTemplate.updateAsynchronously(b5, options);

		/*
		 * Test Single Insert with entity
		 */
		Book b6 = new Book();
		b6.setIsbn("123456-6");
		b6.setTitle("Spring Data Cassandra Book");
		b6.setAuthor("Cassandra Guru");
		b6.setPages(465);

		cassandraDataTemplate.updateAsynchronously(b6, optionsByName);

	}

	@Test
	public void updateBatchTest() {

		QueryOptions options = new QueryOptions();
		options.setConsistencyLevel(ConsistencyLevel.ONE);
		options.setRetryPolicy(RetryPolicy.DOWNGRADING_CONSISTENCY);

		Map<String, Object> optionsByName = new HashMap<String, Object>();
		optionsByName.put(QueryOptions.QueryOptionMapKeys.CONSISTENCY_LEVEL, ConsistencyLevel.ALL);
		optionsByName.put(QueryOptions.QueryOptionMapKeys.RETRY_POLICY, RetryPolicy.FALLTHROUGH);
		optionsByName.put(QueryOptions.QueryOptionMapKeys.TTL, 30);

		List<Book> books = null;

		books = getBookList(20);

		cassandraDataTemplate.insert(books);

		alterBooks(books);

		cassandraDataTemplate.update(books);

		books = getBookList(20);

		cassandraDataTemplate.insert(books, "book_alt");

		alterBooks(books);

		cassandraDataTemplate.update(books, "book_alt");

		books = getBookList(20);

		cassandraDataTemplate.insert(books, "book", options);

		alterBooks(books);

		cassandraDataTemplate.update(books, "book", options);

		books = getBookList(20);

		cassandraDataTemplate.insert(books, "book", optionsByName);

		alterBooks(books);

		cassandraDataTemplate.update(books, "book", optionsByName);

		books = getBookList(20);

		cassandraDataTemplate.insert(books, options);

		alterBooks(books);

		cassandraDataTemplate.update(books, options);

		books = getBookList(20);

		cassandraDataTemplate.insert(books, optionsByName);

		alterBooks(books);

		cassandraDataTemplate.update(books, optionsByName);

	}

	@Test
	public void updateBatchAsynchronouslyTest() {

		QueryOptions options = new QueryOptions();
		options.setConsistencyLevel(ConsistencyLevel.ONE);
		options.setRetryPolicy(RetryPolicy.DOWNGRADING_CONSISTENCY);

		Map<String, Object> optionsByName = new HashMap<String, Object>();
		optionsByName.put(QueryOptions.QueryOptionMapKeys.CONSISTENCY_LEVEL, ConsistencyLevel.ALL);
		optionsByName.put(QueryOptions.QueryOptionMapKeys.RETRY_POLICY, RetryPolicy.FALLTHROUGH);
		optionsByName.put(QueryOptions.QueryOptionMapKeys.TTL, 30);

		List<Book> books = null;

		books = getBookList(20);

		cassandraDataTemplate.insert(books);

		alterBooks(books);

		cassandraDataTemplate.updateAsynchronously(books);

		books = getBookList(20);

		cassandraDataTemplate.insert(books, "book_alt");

		alterBooks(books);

		cassandraDataTemplate.updateAsynchronously(books, "book_alt");

		books = getBookList(20);

		cassandraDataTemplate.insert(books, "book", options);

		alterBooks(books);

		cassandraDataTemplate.updateAsynchronously(books, "book", options);

		books = getBookList(20);

		cassandraDataTemplate.insert(books, "book", optionsByName);

		alterBooks(books);

		cassandraDataTemplate.updateAsynchronously(books, "book", optionsByName);

		books = getBookList(20);

		cassandraDataTemplate.insert(books, options);

		alterBooks(books);

		cassandraDataTemplate.updateAsynchronously(books, options);

		books = getBookList(20);

		cassandraDataTemplate.insert(books, optionsByName);

		alterBooks(books);

		cassandraDataTemplate.updateAsynchronously(books, optionsByName);

	}

	/**
	 * @param books
	 */
	private void alterBooks(List<Book> books) {

		for (Book b : books) {
			b.setAuthor("Ernest Hemmingway");
			b.setTitle("The Old Man and the Sea");
			b.setPages(115);
		}
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

		cassandraDataTemplate.delete(b1);

		Book b2 = new Book();
		b2.setIsbn("123456-2");

		cassandraDataTemplate.delete(b2, "book_alt");

		/*
		 * Test Single Insert with entity
		 */
		Book b3 = new Book();
		b3.setIsbn("123456-3");

		cassandraDataTemplate.delete(b3, "book", options);

		/*
		 * Test Single Insert with entity
		 */
		Book b4 = new Book();
		b4.setIsbn("123456-4");

		cassandraDataTemplate.delete(b4, "book", optionsByName);

		/*
		 * Test Single Insert with entity
		 */
		Book b5 = new Book();
		b5.setIsbn("123456-5");

		cassandraDataTemplate.delete(b5, options);

		/*
		 * Test Single Insert with entity
		 */
		Book b6 = new Book();
		b6.setIsbn("123456-6");

		cassandraDataTemplate.delete(b6, optionsByName);

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

		cassandraDataTemplate.deleteAsynchronously(b1);

		Book b2 = new Book();
		b2.setIsbn("123456-2");

		cassandraDataTemplate.deleteAsynchronously(b2, "book_alt");

		/*
		 * Test Single Insert with entity
		 */
		Book b3 = new Book();
		b3.setIsbn("123456-3");

		cassandraDataTemplate.deleteAsynchronously(b3, "book", options);

		/*
		 * Test Single Insert with entity
		 */
		Book b4 = new Book();
		b4.setIsbn("123456-4");

		cassandraDataTemplate.deleteAsynchronously(b4, "book", optionsByName);

		/*
		 * Test Single Insert with entity
		 */
		Book b5 = new Book();
		b5.setIsbn("123456-5");

		cassandraDataTemplate.deleteAsynchronously(b5, options);

		/*
		 * Test Single Insert with entity
		 */
		Book b6 = new Book();
		b6.setIsbn("123456-6");

		cassandraDataTemplate.deleteAsynchronously(b6, optionsByName);
	}

	@Test
	public void deleteBatchTest() {

		QueryOptions options = new QueryOptions();
		options.setConsistencyLevel(ConsistencyLevel.ONE);
		options.setRetryPolicy(RetryPolicy.DOWNGRADING_CONSISTENCY);

		Map<String, Object> optionsByName = new HashMap<String, Object>();
		optionsByName.put(QueryOptions.QueryOptionMapKeys.CONSISTENCY_LEVEL, ConsistencyLevel.ALL);
		optionsByName.put(QueryOptions.QueryOptionMapKeys.RETRY_POLICY, RetryPolicy.FALLTHROUGH);
		optionsByName.put(QueryOptions.QueryOptionMapKeys.TTL, 30);

		List<Book> books = null;

		books = getBookList(20);

		cassandraDataTemplate.insert(books);

		cassandraDataTemplate.delete(books);

		books = getBookList(20);

		cassandraDataTemplate.insert(books, "book_alt");

		cassandraDataTemplate.delete(books, "book_alt");

		books = getBookList(20);

		cassandraDataTemplate.insert(books, "book", options);

		cassandraDataTemplate.delete(books, "book", options);

		books = getBookList(20);

		cassandraDataTemplate.insert(books, "book", optionsByName);

		cassandraDataTemplate.delete(books, "book", optionsByName);

		books = getBookList(20);

		cassandraDataTemplate.insert(books, options);

		cassandraDataTemplate.delete(books, options);

		books = getBookList(20);

		cassandraDataTemplate.insert(books, optionsByName);

		cassandraDataTemplate.delete(books, optionsByName);

	}

	@Test
	public void deleteBatchAsynchronouslyTest() {

		QueryOptions options = new QueryOptions();
		options.setConsistencyLevel(ConsistencyLevel.ONE);
		options.setRetryPolicy(RetryPolicy.DOWNGRADING_CONSISTENCY);

		Map<String, Object> optionsByName = new HashMap<String, Object>();
		optionsByName.put(QueryOptions.QueryOptionMapKeys.CONSISTENCY_LEVEL, ConsistencyLevel.ALL);
		optionsByName.put(QueryOptions.QueryOptionMapKeys.RETRY_POLICY, RetryPolicy.FALLTHROUGH);
		optionsByName.put(QueryOptions.QueryOptionMapKeys.TTL, 30);

		List<Book> books = null;

		books = getBookList(20);

		cassandraDataTemplate.insert(books);

		cassandraDataTemplate.deleteAsynchronously(books);

		books = getBookList(20);

		cassandraDataTemplate.insert(books, "book_alt");

		cassandraDataTemplate.deleteAsynchronously(books, "book_alt");

		books = getBookList(20);

		cassandraDataTemplate.insert(books, "book", options);

		cassandraDataTemplate.deleteAsynchronously(books, "book", options);

		books = getBookList(20);

		cassandraDataTemplate.insert(books, "book", optionsByName);

		cassandraDataTemplate.deleteAsynchronously(books, "book", optionsByName);

		books = getBookList(20);

		cassandraDataTemplate.insert(books, options);

		cassandraDataTemplate.deleteAsynchronously(books, options);

		books = getBookList(20);

		cassandraDataTemplate.insert(books, optionsByName);

		cassandraDataTemplate.deleteAsynchronously(books, optionsByName);

	}

	@Test
	public void selectOneTest() {

		/*
		 * Test Single Insert with entity
		 */
		Book b1 = new Book();
		b1.setIsbn("123456-1");
		b1.setTitle("Spring Data Cassandra Guide");
		b1.setAuthor("Cassandra Guru");
		b1.setPages(521);

		cassandraDataTemplate.insert(b1);

		Select select = QueryBuilder.select().all().from("book");
		select.where(QueryBuilder.eq("isbn", "123456-1"));

		Book b = cassandraDataTemplate.selectOne(select, Book.class);

		log.info("SingleSelect Book Title -> " + b.getTitle());
		log.info("SingleSelect Book Author -> " + b.getAuthor());

		Assert.assertEquals(b.getTitle(), "Spring Data Cassandra Guide");
		Assert.assertEquals(b.getAuthor(), "Cassandra Guru");

	}

	@Test
	public void selectTest() {

		List<Book> books = getBookList(20);

		cassandraDataTemplate.insert(books);

		Select select = QueryBuilder.select().all().from("book");

		List<Book> b = cassandraDataTemplate.select(select, Book.class);

		log.info("Book Count -> " + b.size());

		Assert.assertEquals(b.size(), 20);

	}

	@Test
	public void selectCountTest() {

		List<Book> books = getBookList(20);

		cassandraDataTemplate.insert(books);

		Select select = QueryBuilder.select().countAll().from("book");

		Long count = cassandraDataTemplate.count(select);

		log.info("Book Count -> " + count);

		Assert.assertEquals(count, new Long(20));

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
