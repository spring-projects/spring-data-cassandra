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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.cassandraunit.CassandraCQLUnit;
import org.cassandraunit.DataLoader;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.cassandraunit.dataset.yaml.ClassPathYamlDataSet;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cassandra.core.ConsistencyLevel;
import org.springframework.cassandra.core.QueryOptions;
import org.springframework.cassandra.core.RetryPolicy;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.test.integration.config.TestConfig;
import org.springframework.data.cassandra.test.integration.support.SpringDataBuildProperties;
import org.springframework.data.cassandra.test.integration.table.Book;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;

/**
 * Unit Tests for CqlTemplate
 * 
 * @author David Webb
 * 
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = { TestConfig.class }, loader = AnnotationConfigContextLoader.class)
public class CassandraDataOperationsTest {

	@Autowired
	private CassandraOperations template;

	private static Logger log = LoggerFactory.getLogger(CassandraDataOperationsTest.class);

	public static final SpringDataBuildProperties PROPS = new SpringDataBuildProperties();
	private final static String CASSANDRA_CONFIG = "spring-cassandra.yaml";
	private final static String KEYSPACE_NAME = "test";
	private final static String CASSANDRA_HOST = "localhost";
	private final static int CASSANDRA_NATIVE_PORT = PROPS.getCassandraPort();
	private final static int CASSANDRA_THRIFT_PORT = PROPS.getCassandraRpcPort();

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

		Book b1 = new Book();
		b1.setIsbn("123456-1");
		b1.setTitle("Spring Data Cassandra Guide");
		b1.setAuthor("Cassandra Guru");
		b1.setPages(521);

		template.insert(b1);

		Book b2 = new Book();
		b2.setIsbn("123456-2");
		b2.setTitle("Spring Data Cassandra Guide");
		b2.setAuthor("Cassandra Guru");
		b2.setPages(521);

		template.insert(b2);

		Book b3 = new Book();
		b3.setIsbn("123456-3");
		b3.setTitle("Spring Data Cassandra Guide");
		b3.setAuthor("Cassandra Guru");
		b3.setPages(265);

		QueryOptions options = new QueryOptions();
		options.setConsistencyLevel(ConsistencyLevel.ONE);
		options.setRetryPolicy(RetryPolicy.DOWNGRADING_CONSISTENCY);

		template.insert(b3, options);

		Book b5 = new Book();
		b5.setIsbn("123456-5");
		b5.setTitle("Spring Data Cassandra Guide");
		b5.setAuthor("Cassandra Guru");
		b5.setPages(265);

		template.insert(b5, options);
	}

	@Test
	public void insertAsynchronouslyTest() {

		Book b1 = new Book();
		b1.setIsbn("123456-1");
		b1.setTitle("Spring Data Cassandra Guide");
		b1.setAuthor("Cassandra Guru");
		b1.setPages(521);

		template.insertAsynchronously(b1);

		Book b2 = new Book();
		b2.setIsbn("123456-2");
		b2.setTitle("Spring Data Cassandra Guide");
		b2.setAuthor("Cassandra Guru");
		b2.setPages(521);

		template.insertAsynchronously(b2);

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

		template.insertAsynchronously(b3, options);

		/*
		 * Test Single Insert with entity
		 */
		Book b4 = new Book();
		b4.setIsbn("123456-4");
		b4.setTitle("Spring Data Cassandra Guide");
		b4.setAuthor("Cassandra Guru");
		b4.setPages(465);

		/*
		 * Test Single Insert with entity
		 */
		Book b5 = new Book();
		b5.setIsbn("123456-5");
		b5.setTitle("Spring Data Cassandra Guide");
		b5.setAuthor("Cassandra Guru");
		b5.setPages(265);

		template.insertAsynchronously(b5, options);

	}

	@Test
	public void insertBatchTest() {

		QueryOptions options = new QueryOptions();
		options.setConsistencyLevel(ConsistencyLevel.ONE);
		options.setRetryPolicy(RetryPolicy.DOWNGRADING_CONSISTENCY);

		List<Book> books = null;

		books = getBookList(20);

		template.insert(books);

		books = getBookList(20);

		template.insert(books);

		books = getBookList(20);

		template.insert(books, options);

		books = getBookList(20);

		template.insert(books, options);

	}

	@Test
	public void insertBatchAsynchronouslyTest() {

		QueryOptions options = new QueryOptions();
		options.setConsistencyLevel(ConsistencyLevel.ONE);
		options.setRetryPolicy(RetryPolicy.DOWNGRADING_CONSISTENCY);

		List<Book> books = null;

		books = getBookList(20);

		template.insertAsynchronously(books);

		books = getBookList(20);

		template.insertAsynchronously(books);

		books = getBookList(20);

		template.insertAsynchronously(books, options);

		books = getBookList(20);

		template.insertAsynchronously(books, options);

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

		/*
		 * Test Single Insert with entity
		 */
		Book b1 = new Book();
		b1.setIsbn("123456-1");
		b1.setTitle("Spring Data Cassandra Book");
		b1.setAuthor("Cassandra Guru");
		b1.setPages(521);

		template.update(b1);

		Book b2 = new Book();
		b2.setIsbn("123456-2");
		b2.setTitle("Spring Data Cassandra Book");
		b2.setAuthor("Cassandra Guru");
		b2.setPages(521);

		template.update(b2);

		/*
		 * Test Single Insert with entity
		 */
		Book b3 = new Book();
		b3.setIsbn("123456-3");
		b3.setTitle("Spring Data Cassandra Book");
		b3.setAuthor("Cassandra Guru");
		b3.setPages(265);

		template.update(b3, options);

		/*
		 * Test Single Insert with entity
		 */
		Book b5 = new Book();
		b5.setIsbn("123456-5");
		b5.setTitle("Spring Data Cassandra Book");
		b5.setAuthor("Cassandra Guru");
		b5.setPages(265);

		template.update(b5, options);

	}

	@Test
	public void updateAsynchronouslyTest() {

		insertTest();

		QueryOptions options = new QueryOptions();
		options.setConsistencyLevel(ConsistencyLevel.ONE);
		options.setRetryPolicy(RetryPolicy.DOWNGRADING_CONSISTENCY);

		/*
		 * Test Single Insert with entity
		 */
		Book b1 = new Book();
		b1.setIsbn("123456-1");
		b1.setTitle("Spring Data Cassandra Book");
		b1.setAuthor("Cassandra Guru");
		b1.setPages(521);

		template.updateAsynchronously(b1);

		Book b2 = new Book();
		b2.setIsbn("123456-2");
		b2.setTitle("Spring Data Cassandra Book");
		b2.setAuthor("Cassandra Guru");
		b2.setPages(521);

		template.updateAsynchronously(b2);

		/*
		 * Test Single Insert with entity
		 */
		Book b3 = new Book();
		b3.setIsbn("123456-3");
		b3.setTitle("Spring Data Cassandra Book");
		b3.setAuthor("Cassandra Guru");
		b3.setPages(265);

		template.updateAsynchronously(b3, options);

		/*
		 * Test Single Insert with entity
		 */
		Book b5 = new Book();
		b5.setIsbn("123456-5");
		b5.setTitle("Spring Data Cassandra Book");
		b5.setAuthor("Cassandra Guru");
		b5.setPages(265);

		template.updateAsynchronously(b5, options);

	}

	@Test
	public void updateBatchTest() {

		QueryOptions options = new QueryOptions();
		options.setConsistencyLevel(ConsistencyLevel.ONE);
		options.setRetryPolicy(RetryPolicy.DOWNGRADING_CONSISTENCY);

		List<Book> books = null;

		books = getBookList(20);

		template.insert(books);

		alterBooks(books);

		template.update(books);

		books = getBookList(20);

		template.insert(books);

		alterBooks(books);

		template.update(books);

		books = getBookList(20);

		template.insert(books, options);

		alterBooks(books);

		template.update(books, options);

		books = getBookList(20);

		template.insert(books, options);

		alterBooks(books);

		template.update(books, options);

	}

	@Test
	public void updateBatchAsynchronouslyTest() {

		QueryOptions options = new QueryOptions();
		options.setConsistencyLevel(ConsistencyLevel.ONE);
		options.setRetryPolicy(RetryPolicy.DOWNGRADING_CONSISTENCY);

		List<Book> books = null;

		books = getBookList(20);

		template.insert(books);

		alterBooks(books);

		template.updateAsynchronously(books);

		books = getBookList(20);

		template.insert(books);

		alterBooks(books);

		template.updateAsynchronously(books);

		books = getBookList(20);

		template.insert(books, options);

		alterBooks(books);

		template.updateAsynchronously(books, options);

		books = getBookList(20);

		template.insert(books, options);

		alterBooks(books);

		template.updateAsynchronously(books, options);

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

		/*
		 * Test Single Insert with entity
		 */
		Book b1 = new Book();
		b1.setIsbn("123456-1");

		template.delete(b1);

		Book b2 = new Book();
		b2.setIsbn("123456-2");

		template.delete(b2);

		/*
		 * Test Single Insert with entity
		 */
		Book b3 = new Book();
		b3.setIsbn("123456-3");

		template.delete(b3, options);

		/*
		 * Test Single Insert with entity
		 */
		Book b5 = new Book();
		b5.setIsbn("123456-5");

		template.delete(b5, options);

	}

	@Test
	public void deleteAsynchronouslyTest() {

		insertTest();

		QueryOptions options = new QueryOptions();
		options.setConsistencyLevel(ConsistencyLevel.ONE);
		options.setRetryPolicy(RetryPolicy.DOWNGRADING_CONSISTENCY);

		/*
		 * Test Single Insert with entity
		 */
		Book b1 = new Book();
		b1.setIsbn("123456-1");

		template.deleteAsynchronously(b1);

		Book b2 = new Book();
		b2.setIsbn("123456-2");

		template.deleteAsynchronously(b2);

		/*
		 * Test Single Insert with entity
		 */
		Book b3 = new Book();
		b3.setIsbn("123456-3");

		template.deleteAsynchronously(b3, options);

		/*
		 * Test Single Insert with entity
		 */
		Book b5 = new Book();
		b5.setIsbn("123456-5");

		template.deleteAsynchronously(b5, options);

	}

	@Test
	public void deleteBatchTest() {

		QueryOptions options = new QueryOptions();
		options.setConsistencyLevel(ConsistencyLevel.ONE);
		options.setRetryPolicy(RetryPolicy.DOWNGRADING_CONSISTENCY);

		List<Book> books = null;

		books = getBookList(20);

		template.insert(books);

		template.delete(books);

		books = getBookList(20);

		template.insert(books);

		template.delete(books);

		books = getBookList(20);

		template.insert(books, options);

		template.delete(books, options);

		books = getBookList(20);

		template.insert(books, options);

		template.delete(books, options);

	}

	@Test
	public void deleteBatchAsynchronouslyTest() {

		QueryOptions options = new QueryOptions();
		options.setConsistencyLevel(ConsistencyLevel.ONE);
		options.setRetryPolicy(RetryPolicy.DOWNGRADING_CONSISTENCY);

		List<Book> books = null;

		books = getBookList(20);

		template.insert(books);

		template.deleteAsynchronously(books);

		books = getBookList(20);

		template.insert(books);

		template.deleteAsynchronously(books);

		books = getBookList(20);

		template.insert(books, options);

		template.deleteAsynchronously(books, options);

		books = getBookList(20);

		template.insert(books, options);

		template.deleteAsynchronously(books, options);

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

		template.insert(b1);

		Select select = QueryBuilder.select().all().from("book");
		select.where(QueryBuilder.eq("isbn", "123456-1"));

		Book b = template.selectOne(select.getQueryString(), Book.class);

		log.info("SingleSelect Book Title -> " + b.getTitle());
		log.info("SingleSelect Book Author -> " + b.getAuthor());

		Assert.assertEquals(b.getTitle(), "Spring Data Cassandra Guide");
		Assert.assertEquals(b.getAuthor(), "Cassandra Guru");

	}

	@Test
	public void selectTest() {

		List<Book> books = getBookList(20);

		template.insert(books);

		Select select = QueryBuilder.select().all().from("book");

		List<Book> b = template.select(select.getQueryString(), Book.class);

		log.info("Book Count -> " + b.size());

		Assert.assertEquals(b.size(), 20);

	}

	@Test
	public void selectCountTest() {

		int count = 20;
		List<Book> books = getBookList(count);

		template.insert(books);

		Assert.assertEquals(count, template.count(Book.class));
	}

	@After
	public void clearCassandra() {
		EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
	}
}
