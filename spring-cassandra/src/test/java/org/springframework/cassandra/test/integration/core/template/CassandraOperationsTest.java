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
package org.springframework.cassandra.test.integration.core.template;

import static org.junit.Assert.assertNotNull;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import junit.framework.Assert;

import org.cassandraunit.CassandraCQLUnit;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cassandra.core.CassandraOperations;
import org.springframework.cassandra.core.CassandraTemplate;
import org.springframework.cassandra.core.HostMapper;
import org.springframework.cassandra.core.PreparedStatementBinder;
import org.springframework.cassandra.core.ResultSetExtractor;
import org.springframework.cassandra.core.RingMember;
import org.springframework.cassandra.core.RowIterator;
import org.springframework.cassandra.test.integration.AbstractEmbeddedCassandraIntegrationTest;
import org.springframework.dao.DataAccessException;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.exceptions.DriverException;

/**
 * Unit Tests for CassandraTemplate
 * 
 * @author David Webb
 * 
 */
public class CassandraOperationsTest extends AbstractEmbeddedCassandraIntegrationTest {

	private CassandraOperations cassandraTemplate;

	private static Logger log = LoggerFactory.getLogger(CassandraOperationsTest.class);

	/*
	 * Objects used for test data
	 */
	final Object[] o1 = new Object[] { "1234", "Moby Dick", "Herman Manville", new Integer(456) };
	final Object[] o2 = new Object[] { "2345", "War and Peace", "Russian Dude", new Integer(456) };
	final Object[] o3 = new Object[] { "3456", "Jane Ayre", "Charlotte", new Integer(456) };

	/**
	 * This loads any test specific Cassandra objects
	 */
	@Rule
	public CassandraCQLUnit cassandraCQLUnit = new CassandraCQLUnit(new ClassPathCQLDataSet(
			"cassandraOperationsTest-cql-dataload.cql", this.keyspace), CASSANDRA_CONFIG, CASSANDRA_HOST,
			CASSANDRA_NATIVE_PORT);

	@Before
	public void setupTemplate() {
		cassandraTemplate = new CassandraTemplate(session);
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
	public void ingestionTestListOfList() {

		String cql = "insert into book (isbn, title, author, pages) values (?, ?, ?, ?)";

		List<List<?>> values = new LinkedList<List<?>>();

		List<Object> l1 = new LinkedList<Object>();
		l1.add("1234");
		l1.add("Moby Dick");
		l1.add("Herman Manville");
		l1.add(new Integer(456));

		values.add(l1);

		List<Object> l2 = new LinkedList<Object>();
		l2.add("2345");
		l2.add("War and Peace");
		l2.add("Russian Dude");
		l2.add(new Integer(456));

		values.add(l2);

		// values.add(new Object[] { "3456", "Jane Ayre", "Charlotte", new Integer(456) });

		cassandraTemplate.ingest(cql, values);

		// Assert that the rows were inserted into Cassandra
		Book b1 = getBook("1234");
		Book b2 = getBook("2345");

		Assert.assertEquals(b1.getIsbn(), l1.get(0));
		Assert.assertEquals(b2.getIsbn(), l2.get(0));
	}

	@Test
	public void ingestionTestObjectArray() {

		String cql = "insert into book (isbn, title, author, pages) values (?, ?, ?, ?)";

		Object[][] values = new Object[3][];
		values[0] = o1;
		values[1] = o2;
		values[2] = o3;

		cassandraTemplate.ingest(cql, values);

		// Assert that the rows were inserted into Cassandra
		Book b1 = getBook("1234");
		Book b2 = getBook("2345");
		Book b3 = getBook("3456");

		Assert.assertEquals(b1.getIsbn(), values[0][0]);
		Assert.assertEquals(b2.getTitle(), values[1][1]);
		Assert.assertEquals(b3.getAuthor(), values[2][2]);
	}

	/**
	 * This is an implementation of RowIterator for the purposes of testing passing your own Impl to CassandraTemplate
	 * 
	 * @author David Webb
	 */
	final class MyRowIterator implements RowIterator {

		private Object[][] values;

		public MyRowIterator(Object[][] values) {
			this.values = values;
		}

		int index = 0;

		/* (non-Javadoc)
		 * @see org.springframework.cassandra.core.RowIterator#next()
		 */
		@Override
		public Object[] next() {
			return values[index++];
		}

		/* (non-Javadoc)
		 * @see org.springframework.cassandra.core.RowIterator#hasNext()
		 */
		@Override
		public boolean hasNext() {
			return index < values.length;
		}

	}

	@Test
	public void ingestionTestRowIterator() {

		String cql = "insert into book (isbn, title, author, pages) values (?, ?, ?, ?)";

		final Object[][] v = new Object[3][];
		v[0] = o1;
		v[1] = o2;
		v[2] = o3;
		RowIterator ri = new MyRowIterator(v);

		cassandraTemplate.ingest(cql, ri);

		// Assert that the rows were inserted into Cassandra
		Book b1 = getBook("1234");
		Book b2 = getBook("2345");
		Book b3 = getBook("3456");

		Assert.assertEquals(b1.getIsbn(), o1[0]);
		Assert.assertEquals(b2.getTitle(), o2[1]);
		Assert.assertEquals(b3.getAuthor(), o3[2]);
	}

	public Book getBook(final String isbn) {

		Book b = this.cassandraTemplate.query("select * from book where isbn = ?", new PreparedStatementBinder() {

			@Override
			public BoundStatement bindValues(PreparedStatement ps) throws DriverException {
				return ps.bind(isbn);
			}
		}, new ResultSetExtractor<Book>() {

			@Override
			public Book extractData(ResultSet rs) throws DriverException, DataAccessException {
				Book b = new Book();
				Row r = rs.one();
				b.setIsbn(r.getString("isbn"));
				b.setTitle(r.getString("title"));
				b.setAuthor(r.getString("author"));
				b.setPages(r.getInt("pages"));
				return b;
			}
		});

		return b;

	}

	/**
	 * For testing a HostMapper Implementation
	 */
	public class MyHost {
		public String someName;
	}
}
