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
package org.springframework.data.cassandra.test.integration.repository;

import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.cassandra.core.CassandraDataOperations;
import org.springframework.data.cassandra.test.integration.table.User;

import com.google.common.collect.Lists;

/**
 * Base class for tests for {@link UserRepository}.
 * 
 * @author Alex Shvid
 * 
 */
// @ContextConfiguration
// @RunWith(SpringJUnit4ClassRunner.class)
public class UserRepositoryIntegrationTests {

	@Autowired
	protected UserRepository repository;

	@Autowired
	protected CassandraDataOperations dataOperations;

	User tom, bob, alice, scott;

	List<User> all;

	@BeforeClass
	public static void startCassandra() throws IOException, TTransportException, ConfigurationException,
			InterruptedException {
		EmbeddedCassandraServerHelper.startEmbeddedCassandra("cassandra.yaml");
	}

	@Before
	public void setUp() throws InterruptedException {

		repository.deleteAll();

		tom = new User();
		tom.setUsername("tom");
		tom.setFirstName("Tom");
		tom.setLastName("Ron");
		tom.setPassword("123");
		tom.setPlace("SF");

		bob = new User();
		bob.setUsername("bob");
		bob.setFirstName("Bob");
		bob.setLastName("White");
		bob.setPassword("555");
		bob.setPlace("NY");

		alice = new User();
		alice.setUsername("alice");
		alice.setFirstName("Alice");
		alice.setLastName("Red");
		alice.setPassword("777");
		alice.setPlace("LA");

		scott = new User();
		scott.setUsername("scott");
		scott.setFirstName("Scott");
		scott.setLastName("Van");
		scott.setPassword("444");
		scott.setPlace("Boston");

		all = dataOperations.insert(Arrays.asList(tom, bob, alice, scott));
	}

	// @Test
	public void findsUserById() throws Exception {

		User user = repository.findOne(bob.getUsername());
		Assert.assertNotNull(user);
		assertEquals(bob, user);

	}

	// @Test
	public void findsAll() throws Exception {
		List<User> result = Lists.newArrayList(repository.findAll());
		assertThat(result.size(), is(all.size()));
		assertThat(result.containsAll(all), is(true));

	}

	// @Test
	public void findsAllWithGivenIds() {

		Iterable<User> result = repository.findAll(Arrays.asList(bob.getUsername(), tom.getUsername()));
		assertThat(result, hasItems(bob, tom));
		assertThat(result, not(hasItems(alice, scott)));
	}

	// @Test
	public void deletesUserCorrectly() throws Exception {

		repository.delete(tom);

		List<User> result = Lists.newArrayList(repository.findAll());

		assertThat(result.size(), is(all.size() - 1));
		assertThat(result, not(hasItem(tom)));
	}

	// @Test
	public void deletesUserByIdCorrectly() {

		repository.delete(tom.getUsername().toString());

		List<User> result = Lists.newArrayList(repository.findAll());

		assertThat(result.size(), is(all.size() - 1));
		assertThat(result, not(hasItem(tom)));
	}

	@AfterClass
	public static void stopCassandra() {
		EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
	}

	private static void assertEquals(User user1, User user2) {
		Assert.assertEquals(user1.getUsername(), user2.getUsername());
		Assert.assertEquals(user1.getFirstName(), user2.getFirstName());
		Assert.assertEquals(user1.getLastName(), user2.getLastName());
		Assert.assertEquals(user1.getPlace(), user2.getPlace());
		Assert.assertEquals(user1.getPassword(), user2.getPassword());
	}

}
