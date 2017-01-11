/*
 * Copyright 2013-2017 the original author or authors
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

import static org.assertj.core.api.Assertions.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import org.springframework.cassandra.core.ConsistencyLevel;
import org.springframework.cassandra.core.QueryOptions;
import org.springframework.cassandra.core.RetryPolicy;
import org.springframework.cassandra.core.WriteOptions;
import org.springframework.cassandra.test.integration.AbstractKeyspaceCreatingIntegrationTest;
import org.springframework.data.cassandra.core.CassandraTemplate;
import org.springframework.data.cassandra.domain.UserToken;
import org.springframework.data.cassandra.repository.support.BasicMapId;
import org.springframework.data.cassandra.test.integration.simpletons.Book;
import org.springframework.data.cassandra.test.integration.simpletons.BookCondition;
import org.springframework.data.cassandra.test.integration.simpletons.BookReference;
import org.springframework.data.cassandra.test.integration.support.SchemaTestUtils;

import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.utils.UUIDs;

/**
 * Integration tests for {@link CassandraTemplate}.
 *
 * @author David Webb
 * @author Mark Paluch
 * @author John Blum
 */
public class CassandraOperationsIntegrationTests extends AbstractKeyspaceCreatingIntegrationTest {

	CassandraTemplate template;

	@Before
	public void before() {

		template = new CassandraTemplate(session);

		SchemaTestUtils.potentiallyCreateTableFor(Book.class, template);
		SchemaTestUtils.potentiallyCreateTableFor(BookReference.class, template);
		SchemaTestUtils.potentiallyCreateTableFor(UserToken.class, template);

		SchemaTestUtils.truncate(Book.class, template);
		SchemaTestUtils.truncate(BookReference.class, template);
		SchemaTestUtils.truncate(UserToken.class, template);
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

		assertThat(list.isEmpty()).isTrue();
	}

	@Test
	public void insertNullList() {
		List<Book> list = template.insert((List<Book>) null);

		assertThat(list).isNull();
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

		assertThat(template.count(Book.class)).isEqualTo(80l);
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

		assertThat(book.getTitle()).isEqualTo("Spring Data Cassandra Guide");
		assertThat(book.getAuthor()).isEqualTo("Cassandra Guru");

	}

	@Test
	public void selectTest() {

		List<Book> books = getBookList(20);

		template.insert(books);

		Select select = QueryBuilder.select().all().from("book");

		List<Book> selectedBooks = template.select(select, Book.class);

		assertThat(selectedBooks).hasSize(20);

		for (Book book : selectedBooks) {
			assertThat(book.isInStock()).isTrue();
			assertThat(book.getCondition()).isEqualTo(BookCondition.NEW);
		}
	}

	@Test
	public void selectCountTest() {

		long count = 20;
		List<Book> books = getBookList(count);

		template.insert(books);

		assertThat(template.count(Book.class)).isEqualTo(count);
	}

	@Test
	public void insertAndSelect() {

		long count = 20;
		List<Book> books = getBookList(count);

		template.insert(books);

		assertThat(template.count(Book.class)).isEqualTo(count);
	}

	@Test // DATACASS-182
	public void updateShouldRemoveFields() {

		Book book = new Book();
		book.setIsbn("isbn");
		book.setTitle("title");
		book.setAuthor("author");

		template.insert(book);

		book.setTitle(null);
		template.update(book);

		Book loaded = template.selectOneById(Book.class, book.getIsbn());

		assertThat(loaded.getTitle()).isNull();
		assertThat(loaded.getAuthor()).isEqualTo("author");
	}

	@Test // DATACASS-182
	public void insertShouldRemoveFields() {

		Book book = new Book();
		book.setIsbn("isbn");
		book.setTitle("title");
		book.setAuthor("author");

		template.insert(book);

		book.setTitle(null);

		template.insert(book);

		Book loaded = template.selectOneById(Book.class, book.getIsbn());

		assertThat(loaded.getTitle()).isNull();
		assertThat(loaded.getAuthor()).isEqualTo("author");
	}

	@Test // DATACASS-182
	public void updateShouldInsertEntity() {

		Book book = new Book();
		book.setIsbn("isbn");
		book.setTitle("title");
		book.setAuthor("author");

		template.update(book);

		Book loaded = template.selectOneById(Book.class, book.getIsbn());

		assertThat(loaded).isNotNull();
		assertThat(loaded.getAuthor()).isEqualTo("author");
		assertThat(loaded.getTitle()).isEqualTo("title");
	}

	@Test // DATACASS-182
	public void insertAndUpdateToEmptyCollection() {

		BookReference bookReference = new BookReference();

		bookReference.setIsbn("isbn");
		bookReference.setBookmarks(Arrays.asList(1, 2, 3, 4));

		template.insert(bookReference);

		bookReference.setBookmarks(Collections.<Integer> emptyList());

		template.update(bookReference);

		BookReference loaded = template.selectOneById(BookReference.class, bookReference.getIsbn());

		assertThat(loaded.getTitle()).isNull();
		assertThat(loaded.getBookmarks()).isNull();
	}

	@Test // DATACASS-182
	public void stream() throws InterruptedException {

		while (template.select("SELECT * FROM book", Book.class).size() != 0) {
			template.truncate("book");
			Thread.sleep(10);
		}

		template.insert(getBookList(20));

		Iterator<Book> iterator = template.stream("SELECT * FROM book", Book.class);

		assertThat(iterator).isNotNull();

		List<Book> selectedBooks = new ArrayList<Book>();

		for (Book book : toIterable(iterator)) {
			selectedBooks.add(book);
		}

		assertThat(selectedBooks).hasSize(20);
		assertThat(selectedBooks.get(0)).isInstanceOf(Book.class);
	}

	@Test // DATACASS-206
	public void shouldUseSpecifiedColumnNamesForSingleEntityModifyingOperations() {

		UserToken userToken = new UserToken();
		userToken.setToken(UUIDs.startOf(System.currentTimeMillis()));
		userToken.setUserId(UUIDs.endOf(System.currentTimeMillis()));

		template.insert(userToken);

		userToken.setUserComment("comment");
		template.update(userToken);

		UserToken loaded = template.selectOneById(UserToken.class,
				BasicMapId.id("userId", userToken.getUserId()).with("token", userToken.getToken()));

		assertThat(loaded).isNotNull();
		assertThat(loaded.getUserComment()).isEqualTo("comment");

		template.delete(userToken);

		UserToken loadAfterDelete = template.selectOneById(UserToken.class,
				BasicMapId.id("userId", userToken.getUserId()).with("token", userToken.getToken()));

		assertThat(loadAfterDelete).isNull();
	}

	@Test // DATACASS-206
	public void shouldUseSpecifiedColumnNamesForMultiEntityModifyingOperations() {

		UserToken userToken = new UserToken();
		userToken.setToken(UUIDs.startOf(System.currentTimeMillis()));
		userToken.setUserId(UUIDs.endOf(System.currentTimeMillis()));

		template.insert(Collections.singletonList(userToken));

		userToken.setUserComment("comment");
		template.update(Collections.singletonList(userToken));

		UserToken loaded = template.selectOneById(UserToken.class,
				BasicMapId.id("userId", userToken.getUserId()).with("token", userToken.getToken()));

		assertThat(loaded).isNotNull();
		assertThat(loaded.getUserComment()).isEqualTo("comment");

		template.delete(Collections.singletonList(userToken));

		UserToken loadAfterDelete = template.selectOneById(UserToken.class,
				BasicMapId.id("userId", userToken.getUserId()).with("token", userToken.getToken()));

		assertThat(loadAfterDelete).isNull();
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
