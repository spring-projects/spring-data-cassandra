/*
 * Copyright 2011-2014 the original author or authors.
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
package org.springframework.data.cassandra.test.integration.collections;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.cassandra.test.integration.simpletons.Book;
import org.springframework.data.cassandra.test.integration.simpletons.BookHistory;
import org.springframework.data.cassandra.test.integration.simpletons.BookReference;
import org.springframework.data.cassandra.test.integration.support.AbstractSpringDataEmbeddedCassandraIntegrationTest;
import org.springframework.data.cassandra.test.integration.support.IntegrationTestConfig;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;

/**
 * @author dwebb
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class CollectionsRowValueProviderTest extends AbstractSpringDataEmbeddedCassandraIntegrationTest {

	@Configuration
	public static class Config extends IntegrationTestConfig {

		@Override
		public String[] getEntityBasePackages() {
			return new String[] { Book.class.getPackage().getName() };
		}
	}

	@Before
	public void before() throws IOException {
		deleteAllEntities();
	}

	@Test
	public void mapTest() {

		BookHistory b1 = new BookHistory();
		b1.setIsbn("123456-1");
		b1.setTitle("Spring Data Cassandra Guide");
		b1.setAuthor("Cassandra Guru");
		b1.setPages(521);
		b1.setSaleDate(new Date());
		b1.setInStock(true);

		Map<String, Integer> checkOutMap = new HashMap<String, Integer>();
		checkOutMap.put("dwebb", 50);
		checkOutMap.put("madams", 100);
		checkOutMap.put("jmcpeek", 150);

		b1.setCheckOuts(checkOutMap);

		template.insert(b1);

		Select select = QueryBuilder.select().all().from("bookHistory");
		select.where(QueryBuilder.eq("isbn", "123456-1"));

		BookHistory b = template.selectOne(select, BookHistory.class);

		Assert.assertNotNull(b.getCheckOuts());

		log.debug("Checkouts map data");
		for (String username : b.getCheckOuts().keySet()) {
			log.debug(username + " has " + b.getCheckOuts().get(username) + " checkouts of this book.");
		}

		Assert.assertEquals(b.getTitle(), "Spring Data Cassandra Guide");
		Assert.assertEquals(b.getAuthor(), "Cassandra Guru");

	}

	@Test
	public void listSetTest() {

		BookReference b1 = new BookReference();
		b1.setIsbn("123456-1");
		b1.setTitle("Spring Data Cassandra Guide");
		b1.setAuthor("Cassandra Guru");
		b1.setPages(521);
		b1.setSaleDate(new Date());
		b1.setInStock(true);

		Set<String> refs = new HashSet<String>();
		refs.add("Spring Data by O'Reilly");
		refs.add("Spring by Example");
		refs.add("Spring Recipies");
		b1.setReferences(refs);

		List<Integer> marks = new LinkedList<Integer>();
		marks.add(13);
		marks.add(52);
		marks.add(144);
		b1.setBookmarks(marks);

		template.insert(b1);

		Select select = QueryBuilder.select().all().from("bookReference");
		select.where(QueryBuilder.eq("isbn", "123456-1"));

		BookReference b = template.selectOne(select, BookReference.class);

		Assert.assertNotNull(b.getReferences());
		Assert.assertNotNull(b.getBookmarks());

		log.debug("Bookmark List<Integer> Data");
		for (Integer mark : b.getBookmarks()) {
			log.debug("Bookmark set on page  " + mark);
		}

		log.debug("Reference Set<String> Data");
		for (String ref : b.getReferences()) {
			log.debug("Reference -> " + ref);
		}

		Assert.assertEquals(b.getTitle(), "Spring Data Cassandra Guide");
		Assert.assertEquals(b.getAuthor(), "Cassandra Guru");

	}
}
