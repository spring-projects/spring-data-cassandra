/*
 * Copyright 2016-2017 the original author or authors.
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
package org.springframework.cassandra.test.integration.core.async;

import static org.assertj.core.api.Assertions.*;
import static org.springframework.cassandra.core.keyspace.CreateTableSpecification.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;

import org.junit.Before;
import org.junit.Test;
import org.springframework.cassandra.core.*;
import org.springframework.cassandra.support.exception.CassandraConnectionFailureException;
import org.springframework.cassandra.test.integration.AbstractKeyspaceCreatingIntegrationTest;
import org.springframework.cassandra.test.integration.support.ListOfMapListener;
import org.springframework.cassandra.test.integration.support.MapListener;
import org.springframework.cassandra.test.integration.support.ObjectListener;
import org.springframework.cassandra.test.integration.support.QueryListener;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;

/**
 * @author Mark Paluch
 */
public class AsynchronousCqlOperationsIntegrationTests extends AbstractKeyspaceCreatingIntegrationTest {

	public static final String TABLE = "book";
	CqlOperations cqlOperations;

	@Before
	public void setUp() {
		cqlOperations = new CqlTemplate(session);
		ensureTableExists();
		cqlOperations.truncate(TABLE);
	}

	public static String cql(Book book, String... columns) {
		if (columns == null || columns.length == 0) {
			columns = new String[] { "title", "isbn" };
		}
		return String.format("select %s from %s where title = '%s' and isbn = '%s'",
				StringUtils.arrayToCommaDelimitedString(columns), TABLE, book.title, book.isbn);
	}

	public static String cql(String[] titles) {
		String[] quoted = new String[titles.length];
		System.arraycopy(titles, 0, quoted, 0, titles.length);
		for (int i = 0; i < quoted.length; i++) {
			quoted[i] = "'" + quoted[i] + "'";
		}

		return String.format("select * from %s where title in (%s)", TABLE,
				StringUtils.arrayToCommaDelimitedString(quoted));
	}

	public static Select select(String isbn) {
		Select select = QueryBuilder.select("isbn", "title").from(TABLE);
		select.where(QueryBuilder.eq("isbn", isbn));
		return select;
	}

	public static final Comparator<Book> BOOK_COMPARATOR = new Comparator<Book>() {
		@Override
		public int compare(Book l, Book r) {
			return l.isbn.compareTo(r.isbn);
		}
	};

	public static final Comparator<? super Map<String, ?>> MAP_WITH_ISBN_COMPARATOR = new Comparator<Map<String, ?>>() {
		@Override
		public int compare(Map<String, ?> o1, Map<String, ?> o2) {
			Assert.isInstanceOf(Comparable.class, o1.get("isbn"),
					"Map o1 must contain a key 'isbn' and a Comparable value to compare the maps");
			Assert.isInstanceOf(Comparable.class, o2.get("isbn"),
					"Map o2 must contain a key 'isbn' and a Comparable value to compare the maps");
			return ((Comparable) o1.get("isbn")).compareTo(o2.get("isbn"));
		}
	};

	public static void assertMapEquals(Map<?, ?> expected, Map<?, ?> actual) {
		for (Object key : expected.keySet()) {
			assertThat(actual.containsKey(key)).isTrue();
			assertThat(actual.get(key)).isEqualTo(expected.get(key));
		}
	}

	void ensureTableExists() {
		cqlOperations.execute(createTable(TABLE).ifNotExists().partitionKeyColumn("title", DataType.ascii())
				.clusteredKeyColumn("isbn", DataType.ascii()));
	}

	Book[] insert(int n) {
		Book[] books = new Book[n];
		for (int i = 0; i < n; i++) {
			Book b = books[i] = Book.random();
			cqlOperations.execute(String.format("insert into %s (isbn, title) values ('%s', '%s')", TABLE, b.isbn, b.title));
		}
		return books;
	}

	void assertBook(Book expected, Book actual) {
		assertThat(actual.isbn).isEqualTo(expected.isbn);
		assertThat(actual.title).isEqualTo(expected.title);
	}

	/**
	 * Tests that test {@link AsynchronousQueryListener} should create an anonymous subclass of this class then call
	 * {@link #test()}.
	 */
	abstract class AsynchronousQueryListenerTestTemplate {

		/**
		 * Subclass must perform the asynchronous query using the given data and listener and set <code>this.expected</code>
		 * to the appropriate value before returning.
		 */
		abstract void doAsyncQuery(Book b, QueryListener listener);

		void test() throws InterruptedException {
			Book expected = insert(1)[0];
			QueryListener listener = QueryListener.create();
			doAsyncQuery(expected, listener);
			listener.await();
			Row r = cqlOperations.getResultSetUninterruptibly(listener.getResultSetFuture()).one();
			Book actual = new Book(r.getString(0), r.getString(1));
			assertBook(expected, actual);
		}
	}

	/**
	 * Tests that test {@link QueryForObjectListener} should create an anonymous subclass of this class then call
	 * {@link #test()}
	 */
	abstract class QueryForObjectListenerTestTemplate<T> {

		/**
		 * Subclass must perform the asynchronous query using the given data and listener and set <code>this.expected</code>
		 * to the appropriate value before returning.
		 */
		abstract void doAsyncQuery(Book b, QueryForObjectListener<T> listener);

		T expected; // subclass should set this value in doAsyncQuery

		void test() throws Exception {
			Book book = insert(1)[0];
			ObjectListener<T> listener = ObjectListener.create();
			doAsyncQuery(book, listener);
			listener.await();
			if (listener.getException() != null) {
				throw listener.getException();
			}
			assertThat(listener.getResult()).isEqualTo(expected);
		}
	}

	/**
	 * Tests that test {@link QueryForMapListener} should create an anonymous subclass of this class then call
	 * {@link #test()}
	 */
	abstract class QueryForMapListenerTestTemplate {

		/**
		 * Subclass must perform the asynchronous query using the given data and listener and set <code>this.expected</code>
		 * to the appropriate value before returning.
		 */
		abstract void doAsyncQuery(Book b, QueryForMapListener listener);

		Map<String, Object> expected; // subclass should set this value in doAsyncQuery

		void test() throws Exception {
			Book book = insert(1)[0];
			MapListener listener = MapListener.create();
			doAsyncQuery(book, listener);
			listener.await();
			if (listener.getException() != null) {
				throw listener.getException();
			}
			assertMapEquals(expected, listener.getResult());
		}
	}

	/**
	 * Tests that test {@link QueryForMapListener} should create an anonymous subclass of this class then call or
	 * {@link #test(int)}
	 */
	abstract class QueryForListListenerTestTemplate {

		/**
		 * Subclass must perform the asynchronous query using the given data and listener and set <code>this.expected</code>
		 * to the appropriate value before returning.
		 */
		abstract void doAsyncQuery(Book[] books, QueryForListOfMapListener listener);

		List<Map<String, ? extends Comparable>> expected; // subclass should set this value in doAsyncQuery

		void test(int n) throws Exception {
			Book[] books = insert(n);
			ListOfMapListener listener = ListOfMapListener.create();
			Arrays.sort(books, BOOK_COMPARATOR);
			doAsyncQuery(books, listener);
			listener.await();
			if (listener.getException() != null) {
				throw listener.getException();
			}

			// sort results the same way as the books array above
			Collections.sort(listener.getResult(), MAP_WITH_ISBN_COMPARATOR);

			for (int i = 0; i < expected.size(); i++) {
				assertMapEquals(expected.get(i), listener.getResult().get(i));
			}
		}
	}

	@Test(expected = CancellationException.class)
	public void testString_AsynchronousQueryListener_Cancelled() throws InterruptedException {
		new AsynchronousQueryListenerTestTemplate() {
			@Override
			void doAsyncQuery(Book b, QueryListener listener) {
				Cancellable qc = cqlOperations.queryAsynchronously(cql(b), listener);
				qc.cancel();
			}
		}.test();
	}

	@Test
	public void testString_AsynchronousQueryListener() throws InterruptedException {
		new AsynchronousQueryListenerTestTemplate() {
			@Override
			void doAsyncQuery(Book b, QueryListener listener) {
				cqlOperations.queryAsynchronously(cql(b), listener);
			}
		}.test();
	}

	public void testString_AsynchronousQueryListener_QueryOptions(final ConsistencyLevel cl) throws InterruptedException {
		new AsynchronousQueryListenerTestTemplate() {
			@Override
			void doAsyncQuery(Book b, QueryListener listener) {
				cqlOperations.queryAsynchronously(cql(b), listener, new QueryOptions(cl, RetryPolicy.DEFAULT));
			}
		}.test();
	}

	@Test
	public void testString_AsynchronousQueryListener_QueryOptionsWithConsistencyLevel1() throws InterruptedException {
		testString_AsynchronousQueryListener_QueryOptions(ConsistencyLevel.ONE);
	}

	@Test(expected = CassandraConnectionFailureException.class)
	public void testString_AsynchronousQueryListener_QueryOptionsWithConsistencyLevel2() throws InterruptedException {
		testString_AsynchronousQueryListener_QueryOptions(ConsistencyLevel.TWO);
	}

	@Test
	public void testSelect_AsynchronousQueryListener() throws InterruptedException {
		new AsynchronousQueryListenerTestTemplate() {
			@Override
			void doAsyncQuery(Book b, QueryListener listener) {
				cqlOperations.queryAsynchronously(cql(b), listener);
			}
		}.test();
	}

	@Test
	public void testString_QueryForObjectListener() throws Exception {
		new QueryForObjectListenerTestTemplate<String>() {

			@Override
			void doAsyncQuery(Book b, QueryForObjectListener<String> listener) {
				cqlOperations.queryForObjectAsynchronously(cql(b, "title"), String.class, listener);
				expected = b.title;
			}

		}.test();
	}

	public void testString_QueryForObjectListener_QueryOptions(final ConsistencyLevel cl) throws Exception {
		new QueryForObjectListenerTestTemplate<String>() {

			@Override
			void doAsyncQuery(Book b, QueryForObjectListener<String> listener) {
				QueryOptions opts = new QueryOptions(cl, RetryPolicy.LOGGING);
				cqlOperations.queryForObjectAsynchronously(cql(b, "title"), String.class, listener, opts);
				expected = b.title;
			}

		}.test();
	}

	@Test
	public void testString_QueryForObjectListener_QueryOptionsWithConsistencyLevel() throws Exception {
		testString_QueryForObjectListener_QueryOptions(ConsistencyLevel.ONE);
	}

	@Test(expected = CassandraConnectionFailureException.class)
	public void testString_QueryForObjectListener_QueryOptionsWithConsistencyLevel2() throws Exception {
		testString_QueryForObjectListener_QueryOptions(ConsistencyLevel.TWO);
	}

	@Test
	public void testString_QueryForMapListener() throws Exception {
		new QueryForMapListenerTestTemplate() {

			@Override
			void doAsyncQuery(Book b, QueryForMapListener listener) {
				cqlOperations.queryForMapAsynchronously(cql(b), listener);
				expected = new HashMap<String, Object>();
				expected.put("isbn", b.isbn);
				expected.put("title", b.title);
			}

		}.test();
	}

	public void testString_QueryForMapListener_QueryOptions(final ConsistencyLevel cl) throws Exception {
		new QueryForMapListenerTestTemplate() {

			@Override
			void doAsyncQuery(Book b, QueryForMapListener listener) {
				QueryOptions opts = new QueryOptions(cl, RetryPolicy.LOGGING);
				cqlOperations.queryForMapAsynchronously(cql(b), listener, opts);
				expected = new HashMap<String, Object>();
				expected.put("isbn", b.isbn);
				expected.put("title", b.title);
			}

		}.test();
	}

	@Test
	public void testString_QueryForMapListener_QueryOptionsWithConsistencyLevel1() throws Exception {
		testString_QueryForMapListener_QueryOptions(ConsistencyLevel.ONE);
	}

	@Test(expected = CassandraConnectionFailureException.class)
	public void testString_QueryForMapListener_QueryOptionsWithConsistencyLevel2() throws Exception {
		testString_QueryForMapListener_QueryOptions(ConsistencyLevel.TWO);
	}

	@Test
	public void testString_QueryForListListener() throws Exception {
		new QueryForListListenerTestTemplate() {

			@Override
			void doAsyncQuery(Book[] books, QueryForListOfMapListener listener) {

				String[] titles = new String[books.length];
				expected = new ArrayList<Map<String, ? extends Comparable>>(books.length);
				for (int i = 0; i < books.length; i++) {
					Book b = books[i];
					titles[i] = b.title;
					Map<String, String> row = new HashMap<String, String>(2);
					row.put("title", b.title);
					row.put("isbn", b.isbn);
					expected.add(row);
				}

				cqlOperations.queryForListOfMapAsynchronously(cql(titles), listener);
			}

		}.test(2);
	}

	public void testString_QueryForListListener_QueryOptions(final ConsistencyLevel cl) throws Exception {
		new QueryForListListenerTestTemplate() {

			@Override
			void doAsyncQuery(Book[] books, QueryForListOfMapListener listener) {

				String[] titles = new String[books.length];
				expected = new ArrayList<Map<String, ? extends Comparable>>(books.length);
				for (int i = 0; i < books.length; i++) {
					Book b = books[i];
					titles[i] = b.title;
					Map<String, String> row = new HashMap<String, String>(2);
					row.put("title", b.title);
					row.put("isbn", b.isbn);
					expected.add(row);
				}

				cqlOperations.queryForListOfMapAsynchronously(cql(titles), listener, new QueryOptions(cl, RetryPolicy.LOGGING));

			}

		}.test(2);
	}

	@Test
	public void testString_QueryForListListener_QueryOptionsWithConsistencyLevel1() throws Exception {
		testString_QueryForListListener_QueryOptions(ConsistencyLevel.ONE);
	}

	@Test(expected = CassandraConnectionFailureException.class)
	public void testString_QueryForListListener_QueryOptionsWithConsistencyLevel2() throws Exception {
		testString_QueryForListListener_QueryOptions(ConsistencyLevel.TWO);
	}
}
