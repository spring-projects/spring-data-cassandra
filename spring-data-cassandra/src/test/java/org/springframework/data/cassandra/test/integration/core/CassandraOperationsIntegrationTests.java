/*
 * Copyright 2013-2016 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.test.integration.core;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cassandra.core.ConsistencyLevel;
import org.springframework.cassandra.core.QueryOptions;
import org.springframework.cassandra.core.RetryPolicy;
import org.springframework.cassandra.core.WriteOptions;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.domain.UserToken;
import org.springframework.data.cassandra.repository.support.BasicMapId;
import org.springframework.data.cassandra.test.integration.simpletons.Book;
import org.springframework.data.cassandra.test.integration.simpletons.BookCondition;
import org.springframework.data.cassandra.test.integration.simpletons.BookReference;
import org.springframework.data.cassandra.test.integration.support.AbstractSpringDataEmbeddedCassandraIntegrationTest;
import org.springframework.data.cassandra.test.integration.support.IntegrationTestConfig;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.utils.UUIDs;

/**
 * Integration tests for {@link CassandraOperations}.
 *
 * @author David Webb
 * @author Mark Paluch
 * @author John Blum
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class CassandraOperationsIntegrationTests extends AbstractSpringDataEmbeddedCassandraIntegrationTest {

	@Configuration
	public static class Config extends IntegrationTestConfig {

		@Override
		public String[] getEntityBasePackages() {
			return new String[] { Book.class.getPackage().getName(), UserToken.class.getPackage().getName() };
		}
	}

	@Autowired CassandraOperations template;

	@Before
	public void before() {
		deleteAllEntities();
	}

	@Test
	public void insertTest() {

		Book b1 = new Book();
		b1.setIsbn("123456-1");
		b1.setTitle("Spring Data Cassandra Guide");
		b1.setAuthor("Cassandra Guru");
		b1.setPages(521);
		b1.setSaleDate(new Date());
		b1.setInStock(true);
		b1.setCondition(BookCondition.NEW);

		template.insert(b1);

		Book b2 = new Book();
		b2.setIsbn("123456-2");
		b2.setTitle("Spring Data Cassandra Guide");
		b2.setAuthor("Cassandra Guru");
		b2.setPages(521);
		b2.setCondition(BookCondition.NEW);

		template.insert(b2);

		Book b3 = new Book();
		b3.setIsbn("123456-3");
		b3.setTitle("Spring Data Cassandra Guide");
		b3.setAuthor("Cassandra Guru");
		b3.setPages(265);
		b3.setCondition(BookCondition.USED);

		WriteOptions options = newWriteOptions(ConsistencyLevel.ONE, RetryPolicy.DOWNGRADING_CONSISTENCY, 60);

		template.insert(b3, options);

		Book b5 = new Book();
		b5.setIsbn("123456-5");
		b5.setTitle("Spring Data Cassandra Guide");
		b5.setAuthor("Cassandra Guru");
		b5.setPages(265);
		b5.setCondition(BookCondition.USED);

		template.insert(b5, options);
	}

	@Test
	@SuppressWarnings("deprecation")
	public void insertAsynchronouslyTest() {

		Book b1 = new Book();
		b1.setIsbn("123456-1");
		b1.setTitle("Spring Data Cassandra Guide");
		b1.setAuthor("Cassandra Guru");
		b1.setPages(521);
		b1.setCondition(BookCondition.NEW);

		template.insertAsynchronously(b1);

		Book b2 = new Book();
		b2.setIsbn("123456-2");
		b2.setTitle("Spring Data Cassandra Guide");
		b2.setAuthor("Cassandra Guru");
		b2.setPages(521);
		b2.setCondition(BookCondition.NEW);

		template.insertAsynchronously(b2);

		/*
		 * Test Single Insert with entity
		 */
		Book b3 = new Book();
		b3.setIsbn("123456-3");
		b3.setTitle("Spring Data Cassandra Guide");
		b3.setAuthor("Cassandra Guru");
		b3.setPages(265);
		b3.setCondition(BookCondition.USED);

		WriteOptions options = newWriteOptions(ConsistencyLevel.ONE, RetryPolicy.DOWNGRADING_CONSISTENCY, 60);

		template.insertAsynchronously(b3, options);

		/*
		 * Test Single Insert with entity
		 */
		Book b4 = new Book();
		b4.setIsbn("123456-4");
		b4.setTitle("Spring Data Cassandra Guide");
		b4.setAuthor("Cassandra Guru");
		b4.setPages(465);
		b4.setCondition(BookCondition.USED);

		/*
		 * Test Single Insert with entity
		 */
		Book b5 = new Book();
		b5.setIsbn("123456-5");
		b5.setTitle("Spring Data Cassandra Guide");
		b5.setAuthor("Cassandra Guru");
		b5.setPages(265);
		b5.setCondition(BookCondition.USED);

		template.insertAsynchronously(b5, options);
	}

	@Test
	public void insertEmptyList() {
		List<Book> list = template.insert(new ArrayList<Book>());

		assertThat(list, is(notNullValue(List.class)));
		assertThat(list.isEmpty(), is(true));
	}

	@Test
	public void insertNullList() {
		List<Book> list = template.insert((List<Book>) null);

		assertThat(list, is(nullValue(List.class)));
	}

	@Test
	public void insertBatchTest() {

		WriteOptions options = newWriteOptions(ConsistencyLevel.ONE, RetryPolicy.DOWNGRADING_CONSISTENCY, 60);

		List<Book> books = getBookList(20);

		template.insert(books);

		books = getBookList(20);

		template.insert(books);

		books = getBookList(20);

		template.insert(books, options);

		books = getBookList(20);

		template.insert(books, options);

		assertThat(template.count(Book.class), is(equalTo(80l)));
	}

	@Test
	@SuppressWarnings("deprecation")
	public void insertBatchAsynchronouslyTest() {

		WriteOptions options = newWriteOptions(ConsistencyLevel.ONE, RetryPolicy.DOWNGRADING_CONSISTENCY, 60);

		List<Book> books = getBookList(20);

		template.insertAsynchronously(books);

		books = getBookList(20);

		template.insertAsynchronously(books);

		books = getBookList(20);

		template.insertAsynchronously(books, options);

		books = getBookList(20);

		template.insertAsynchronously(books, options);
	}

	private List<Book> getBookList(long numBooks) {

		List<Book> books = new ArrayList<Book>();
		Book book;

		for (int index = 0; index < numBooks; index++) {
			book = new Book();
			book.setIsbn(UUID.randomUUID().toString());
			book.setTitle("Spring Data Cassandra Guide");
			book.setAuthor("Cassandra Guru");
			book.setPages(index * 10 + 5);
			book.setInStock(true);
			book.setSaleDate(new Date());
			book.setCondition(BookCondition.NEW);
			books.add(book);
		}

		return books;
	}

	@Test
	public void updateTest() {

		insertTest();

		WriteOptions options = newWriteOptions(ConsistencyLevel.ONE, RetryPolicy.DOWNGRADING_CONSISTENCY, 60);

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
	@SuppressWarnings("deprecation")
	public void updateAsynchronouslyTest() {

		insertTest();

		WriteOptions options = newWriteOptions(ConsistencyLevel.ONE, RetryPolicy.DOWNGRADING_CONSISTENCY, 60);

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

		WriteOptions options = newWriteOptions(ConsistencyLevel.ONE, RetryPolicy.DOWNGRADING_CONSISTENCY, 60);

		List<Book> books = getBookList(20);

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
	@SuppressWarnings("deprecation")
	public void updateBatchAsynchronouslyTest() {

		WriteOptions options = newWriteOptions(ConsistencyLevel.ONE, RetryPolicy.DOWNGRADING_CONSISTENCY, 60);

		List<Book> books = getBookList(20);

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

	private void alterBooks(List<Book> books) {

		for (Book book : books) {
			book.setAuthor("Ernest Hemmingway");
			book.setTitle("The Old Man and the Sea");
			book.setPages(115);
		}
	}

	@Test
	public void deleteTest() {

		insertTest();

		QueryOptions options = new QueryOptions();
		options.setConsistencyLevel(ConsistencyLevel.ONE);
		options.setRetryPolicy(RetryPolicy.DOWNGRADING_CONSISTENCY);

		// Test Single Insert with entity
		Book b1 = new Book();
		b1.setIsbn("123456-1");

		template.delete(b1);

		Book b2 = new Book();
		b2.setIsbn("123456-2");

		template.delete(b2);

		// Test Single Insert with entity
		Book b3 = new Book();
		b3.setIsbn("123456-3");

		template.delete(b3, options);

		// Test Single Insert with entity
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

		WriteOptions options = newWriteOptions(ConsistencyLevel.ONE, RetryPolicy.DOWNGRADING_CONSISTENCY, 60);

		List<Book> books = getBookList(20);

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

		WriteOptions options = newWriteOptions(ConsistencyLevel.ONE, RetryPolicy.DOWNGRADING_CONSISTENCY, 60);

		List<Book> books = getBookList(20);

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

		Book book = template.selectOne(select, Book.class);

		log.debug("SingleSelect Book Title -> " + book.getTitle());
		log.debug("SingleSelect Book Author -> " + book.getAuthor());

		assertThat(book.getTitle(), is(equalTo("Spring Data Cassandra Guide")));
		assertThat(book.getAuthor(), is(equalTo("Cassandra Guru")));

	}

	@Test
	public void selectTest() {

		List<Book> books = getBookList(20);

		template.insert(books);

		Select select = QueryBuilder.select().all().from("book");

		List<Book> selectedBooks = template.select(select, Book.class);

		log.debug("Book Count -> " + selectedBooks.size());

		assertThat(selectedBooks.size(), is(equalTo(20)));

		for (Book book : selectedBooks) {
			assertThat(book.isInStock(), is(true));
			assertThat(book.getCondition(), is(equalTo(BookCondition.NEW)));
		}
	}

	@Test
	public void selectCountTest() {

		long count = 20;
		List<Book> books = getBookList(count);

		template.insert(books);

		assertThat(template.count(Book.class), is(equalTo(count)));
	}

	@Test
	public void insertAndSelect() {

		long count = 20;
		List<Book> books = getBookList(count);

		template.insert(books);

		assertThat(template.count(Book.class), is(equalTo(count)));
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-182">DATACASS-182</a>
	 */
	@Test
	public void updateShouldRemoveFields() {

		Book book = new Book();
		book.setIsbn("isbn");
		book.setTitle("title");
		book.setAuthor("author");

		template.insert(book);

		book.setTitle(null);
		template.update(book);

		Book loaded = template.selectOneById(Book.class, book.getIsbn());

		assertThat(loaded.getTitle(), is(nullValue()));
		assertThat(loaded.getAuthor(), is(equalTo("author")));
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-182">DATACASS-182</a>
	 */
	@Test
	public void insertShouldRemoveFields() {

		Book book = new Book();
		book.setIsbn("isbn");
		book.setTitle("title");
		book.setAuthor("author");

		template.insert(book);

		book.setTitle(null);

		template.insert(book);

		Book loaded = template.selectOneById(Book.class, book.getIsbn());

		assertThat(loaded.getTitle(), is(nullValue()));
		assertThat(loaded.getAuthor(), is(equalTo("author")));
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-182">DATACASS-182</a>
	 */
	@Test
	public void updateShouldInsertEntity() {

		Book book = new Book();
		book.setIsbn("isbn");
		book.setTitle("title");
		book.setAuthor("author");

		template.update(book);

		Book loaded = template.selectOneById(Book.class, book.getIsbn());

		assertThat(loaded, is(notNullValue()));
		assertThat(loaded.getAuthor(), is(equalTo("author")));
		assertThat(loaded.getTitle(), is(equalTo("title")));
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-182">DATACASS-182</a>
	 */
	@Test
	public void insertAndUpdateToEmptyCollection() {

		BookReference bookReference = new BookReference();

		bookReference.setIsbn("isbn");
		bookReference.setBookmarks(Arrays.asList(1, 2, 3, 4));

		template.insert(bookReference);

		bookReference.setBookmarks(Collections.<Integer> emptyList());

		template.update(bookReference);

		BookReference loaded = template.selectOneById(BookReference.class, bookReference.getIsbn());

		assertThat(loaded.getTitle(), is(nullValue()));
		assertThat(loaded.getBookmarks(), is(nullValue()));
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-182">DATACASS-182</a>
	 */
	@Test
	public void stream() {

		template.insert(getBookList(20));

		Iterator<Book> iterator = template.stream("SELECT * FROM book", Book.class);

		assertThat(iterator, is(notNullValue(Iterator.class)));

		List<Book> selectedBooks = new ArrayList<Book>();

		for (Book book : toIterable(iterator)) {
			selectedBooks.add(book);
		}

		assertThat(selectedBooks.size(), is(equalTo(20)));
		assertThat(selectedBooks.get(0), is(instanceOf(Book.class)));
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-206">DATACASS-206</a>
	 */
	@Test
	public void shouldUseSpecifiedColumnNamesForSingleEntityModifyingOperations() {

		UserToken userToken = new UserToken();
		userToken.setToken(UUIDs.startOf(System.currentTimeMillis()));
		userToken.setUserId(UUIDs.endOf(System.currentTimeMillis()));

		template.insert(userToken);

		userToken.setUserComment("comment");
		template.update(userToken);

		UserToken loaded = template.selectOneById(UserToken.class,
				BasicMapId.id("userId", userToken.getUserId()).with("token", userToken.getToken()));

		assertThat(loaded, is(notNullValue()));
		assertThat(loaded.getUserComment(), is(equalTo("comment")));

		template.delete(userToken);

		UserToken loadAfterDelete = template.selectOneById(UserToken.class,
				BasicMapId.id("userId", userToken.getUserId()).with("token", userToken.getToken()));

		assertThat(loadAfterDelete, is(nullValue()));
	}

	/**
	 * @see <a href="https://jira.spring.io/browse/DATACASS-206">DATACASS-206</a>
	 */
	@Test
	public void shouldUseSpecifiedColumnNamesForMultiEntityModifyingOperations() {

		UserToken userToken = new UserToken();
		userToken.setToken(UUIDs.startOf(System.currentTimeMillis()));
		userToken.setUserId(UUIDs.endOf(System.currentTimeMillis()));

		template.insert(Arrays.asList(userToken));

		userToken.setUserComment("comment");
		template.update(Arrays.asList(userToken));

		UserToken loaded = template.selectOneById(UserToken.class,
				BasicMapId.id("userId", userToken.getUserId()).with("token", userToken.getToken()));

		assertThat(loaded, is(notNullValue()));
		assertThat(loaded.getUserComment(), is(equalTo("comment")));

		template.delete(Arrays.asList(userToken));

		UserToken loadAfterDelete = template.selectOneById(UserToken.class,
				BasicMapId.id("userId", userToken.getUserId()).with("token", userToken.getToken()));

		assertThat(loadAfterDelete, is(nullValue()));
	}

	WriteOptions newWriteOptions(ConsistencyLevel consistencyLevel, RetryPolicy retryPolicy, int timeToLive) {
		return new WriteOptions(consistencyLevel, retryPolicy, timeToLive);
	}

	<T> Iterable<T> toIterable(final Iterator<T> iterator) {
		return new Iterable<T>() {
			@Override
			public Iterator<T> iterator() {
				return iterator;
			}
		};
	}
}
