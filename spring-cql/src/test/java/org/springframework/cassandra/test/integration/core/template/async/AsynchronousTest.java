package org.springframework.cassandra.test.integration.core.template.async;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.springframework.cassandra.core.keyspace.CreateTableSpecification.createTable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;

import org.junit.Before;
import org.junit.Test;
import org.springframework.cassandra.core.AsynchronousQueryListener;
import org.springframework.cassandra.core.Cancellable;
import org.springframework.cassandra.core.ConsistencyLevel;
import org.springframework.cassandra.core.QueryForListOfMapListener;
import org.springframework.cassandra.core.QueryForMapListener;
import org.springframework.cassandra.core.QueryForObjectListener;
import org.springframework.cassandra.core.QueryOptions;
import org.springframework.cassandra.core.RetryPolicy;
import org.springframework.cassandra.support.exception.CassandraConnectionFailureException;
import org.springframework.util.StringUtils;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;

public class AsynchronousTest extends AbstractAsynchronousTest {

	public static final String TABLE = "book";

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

		return String
				.format("select * from %s where title in (%s)", TABLE, StringUtils.arrayToCommaDelimitedString(quoted));
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

	public static void assertMapEquals(Map<?, ?> expected, Map<?, ?> actual) {
		for (Object key : expected.keySet()) {
			assertTrue(actual.containsKey(key));
			assertEquals(expected.get(key), actual.get(key));
		}
	}

	void ensureTableExists() {
		t.execute(createTable(TABLE).ifNotExists().partitionKeyColumn("title", DataType.ascii())
				.clusteredKeyColumn("isbn", DataType.ascii()));
	}

	Book[] insert(int n) {
		Book[] books = new Book[n];
		for (int i = 0; i < n; i++) {
			Book b = books[i] = Book.random();
			t.execute(String.format("insert into %s (isbn, title) values ('%s', '%s')", TABLE, b.isbn, b.title));
		}
		return books;
	}

	@Before
	public void beforeEach() {
		ensureTableExists();
		t.truncate(TABLE);
	}

	void assertBook(Book expected, Book actual) {
		assertEquals(expected.isbn, actual.isbn);
		assertEquals(expected.title, actual.title);
	}

	/**
	 * Tests that test {@link AsynchronousQueryListener} should create an anonymous subclass of this class then call
	 * either {@link #test()} or {@link #test(int)}
	 */
	abstract class AsynchronousQueryListenerTestTemplate {

		/**
		 * Subclass must perform the asynchronous query using the given data and listener and set <code>this.expected</code>
		 * to the appropriate value before returning.
		 */
		abstract void doAsyncQuery(Book b, BasicListener listener);

		void test() throws InterruptedException {
			Book expected = insert(1)[0];
			BasicListener listener = new BasicListener();
			doAsyncQuery(expected, listener);
			listener.await();
			Row r = t.getResultSetUninterruptibly(listener.rsf).one();
			Book actual = new Book(r.getString(0), r.getString(1));
			assertBook(expected, actual);
		}
	}

	/**
	 * Tests that test {@link QueryForObjectListener} should create an anonymous subclass of this class then call either
	 * {@link #test()} or {@link #test(int)}
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
			ObjectListener<T> listener = new ObjectListener<T>();
			doAsyncQuery(book, listener);
			listener.await();
			if (listener.exception != null) {
				throw listener.exception;
			}
			assertEquals(expected, listener.result);
		}
	}

	/**
	 * Tests that test {@link QueryForMapListener} should create an anonymous subclass of this class then call either
	 * {@link #test()} or {@link #test(int)}
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
			MapListener listener = new MapListener();
			doAsyncQuery(book, listener);
			listener.await();
			if (listener.exception != null) {
				throw listener.exception;
			}
			assertMapEquals(expected, listener.result);
		}
	}

	/**
	 * Tests that test {@link QueryForMapListener} should create an anonymous subclass of this class then call either
	 * {@link #test()} or {@link #test(int)}
	 */
	abstract class QueryForListListenerTestTemplate {

		/**
		 * Subclass must perform the asynchronous query using the given data and listener and set <code>this.expected</code>
		 * to the appropriate value before returning.
		 */
		abstract void doAsyncQuery(Book[] books, QueryForListOfMapListener listener);

		List<Map<String, Object>> expected; // subclass should set this value in doAsyncQuery

		void test(int n) throws Exception {
			Book[] books = insert(n);
			ListOfMapListener listener = new ListOfMapListener();
			Arrays.sort(books, BOOK_COMPARATOR);
			doAsyncQuery(books, listener);
			listener.await();
			if (listener.exception != null) {
				throw listener.exception;
			}

			for (int i = 0; i < expected.size(); i++) {
				assertMapEquals(expected.get(i), listener.result.get(i));
			}
		}
	}

	@Test(expected = CancellationException.class)
	public void testString_AsynchronousQueryListener_Cancelled() throws InterruptedException {
		new AsynchronousQueryListenerTestTemplate() {
			@Override
			void doAsyncQuery(Book b, BasicListener listener) {
				Cancellable qc = t.queryAsynchronously(cql(b), listener);
				qc.cancel();
			}
		}.test();
	}

	@Test
	public void testString_AsynchronousQueryListener() throws InterruptedException {
		new AsynchronousQueryListenerTestTemplate() {
			@Override
			void doAsyncQuery(Book b, BasicListener listener) {
				t.queryAsynchronously(cql(b), listener);
			}
		}.test();
	}

	public void testString_AsynchronousQueryListener_QueryOptions(final ConsistencyLevel cl) throws InterruptedException {
		new AsynchronousQueryListenerTestTemplate() {
			@Override
			void doAsyncQuery(Book b, BasicListener listener) {
				t.queryAsynchronously(cql(b), listener, new QueryOptions(cl, RetryPolicy.LOGGING));
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
			void doAsyncQuery(Book b, BasicListener listener) {
				t.queryAsynchronously(cql(b), listener);
			}
		}.test();
	}

	@Test
	public void testString_QueryForObjectListener() throws Exception {
		new QueryForObjectListenerTestTemplate<String>() {

			@Override
			void doAsyncQuery(Book b, QueryForObjectListener<String> listener) {
				t.queryForObjectAsynchronously(cql(b, "title"), String.class, listener);
				expected = b.title;
			}

		}.test();
	}

	public void testString_QueryForObjectListener_QueryOptions(final ConsistencyLevel cl) throws Exception {
		new QueryForObjectListenerTestTemplate<String>() {

			@Override
			void doAsyncQuery(Book b, QueryForObjectListener<String> listener) {
				QueryOptions opts = new QueryOptions(cl, RetryPolicy.LOGGING);
				t.queryForObjectAsynchronously(cql(b, "title"), String.class, listener, opts);
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
				t.queryForMapAsynchronously(cql(b), listener);
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
				t.queryForMapAsynchronously(cql(b), listener, opts);
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
				expected = new ArrayList<Map<String, Object>>(books.length);
				for (int i = 0; i < books.length; i++) {
					Book b = books[i];
					titles[i] = b.title;
					HashMap<String, Object> row = new HashMap<String, Object>(2);
					row.put("title", b.title);
					row.put("isbn", b.isbn);
					expected.add(row);
				}

				t.queryForListOfMapAsynchronously(cql(titles), listener);
			}

		}.test(2);
	}

	public void testString_QueryForListListener_QueryOptions(final ConsistencyLevel cl) throws Exception {
		new QueryForListListenerTestTemplate() {

			@Override
			void doAsyncQuery(Book[] books, QueryForListOfMapListener listener) {

				String[] titles = new String[books.length];
				expected = new ArrayList<Map<String, Object>>(books.length);
				for (int i = 0; i < books.length; i++) {
					Book b = books[i];
					titles[i] = b.title;
					HashMap<String, Object> row = new HashMap<String, Object>(2);
					row.put("title", b.title);
					row.put("isbn", b.isbn);
					expected.add(row);
				}

				t.queryForListOfMapAsynchronously(cql(titles), listener, new QueryOptions(cl, RetryPolicy.LOGGING));
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
