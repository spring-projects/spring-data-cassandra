package org.springframework.data.cassandra.test.integration.querymethods.declared;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.cassandra.config.SchemaAction;
import org.springframework.data.cassandra.test.integration.querymethods.declared.base.PersonRepository;
import org.springframework.data.cassandra.test.integration.support.AbstractSpringDataEmbeddedCassandraIntegrationTest;
import org.springframework.data.cassandra.test.integration.support.IntegrationTestConfig;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
public abstract class QueryIntegrationTests extends AbstractSpringDataEmbeddedCassandraIntegrationTest {

	public static class Config extends IntegrationTestConfig {

		@Override
		public String[] getEntityBasePackages() {
			return new String[] { Person.class.getPackage().getName() };
		}

		@Override
		public SchemaAction getSchemaAction() {
			return SchemaAction.RECREATE_DROP_UNUSED;
		}
	}

	@Autowired
	PersonRepository r;

	@Before
	public void before() {
		deleteAllEntities();
	}

	@Test
	public void testListMethodSingleResult() {

		Person saved = new Person();
		saved.setFirstname(uuid());
		saved.setLastname(uuid());

		saved = r.save(saved);

		List<Person> results = r.findFolksWithLastnameAsList(saved.getLastname());

		assertNotNull(results);
		assertTrue(results.size() == 1);
		Person found = results.iterator().next();
		assertNotNull(found);
		assertEquals(found.getLastname(), saved.getLastname());
		assertEquals(found.getFirstname(), saved.getFirstname());
	}

	@Test
	public void testListMethodMultipleResults() {

		Person saved = new Person();
		saved.setFirstname("a");
		saved.setLastname(uuid());

		saved = r.save(saved);

		Person saved2 = new Person();
		saved2.setFirstname("b");
		saved2.setLastname(saved.getLastname());

		saved2 = r.save(saved2);

		List<Person> results = r.findFolksWithLastnameAsList(saved.getLastname());

		assertNotNull(results);
		assertTrue(results.size() == 2);
		boolean first = true;
		for (Person person : results) {
			assertNotNull(person);
			assertEquals(saved.getLastname(), person.getLastname());
			assertEquals(first ? saved.getFirstname() : saved2.getFirstname(), person.getFirstname());
			first = false;
		}
	}

	@Test
	public void testListOfMapOfStringToObjectMethodSingleResult() {

		Person saved = new Person();
		saved.setFirstname(uuid());
		saved.setLastname(uuid());

		saved = r.save(saved);

		List<Map<String, Object>> results = r.findFolksWithLastnameAsListOfMapOfStringToObject(saved.getLastname());

		assertNotNull(results);
		assertTrue(results.size() == 1);
		Map<String, Object> found = results.iterator().next();
		assertNotNull(found);
		assertEquals(found.get("lastname"), saved.getLastname());
		assertEquals(found.get("firstname"), saved.getFirstname());
	}

	@Test
	public void testEntityMethodResult() {

		Person saved = new Person();
		saved.setFirstname(uuid());
		saved.setLastname(uuid());

		saved = r.save(saved);

		Person found = r.findSingle(saved.getLastname(), saved.getFirstname());

		assertNotNull(found);
		assertEquals(found.getLastname(), saved.getLastname());
		assertEquals(found.getFirstname(), saved.getFirstname());
	}

	@Test
	public void testListOfMapOfStringToObjectMethodMultipleResults() {

		Person saved = new Person();
		saved.setFirstname("a");
		saved.setLastname(uuid());

		saved = r.save(saved);

		Person saved2 = new Person();
		saved2.setFirstname("b");
		saved2.setLastname(saved.getLastname());

		saved2 = r.save(saved2);

		Collection<Person> results = r.findFolksWithLastnameAsList(saved.getLastname());

		assertNotNull(results);
		assertTrue(results.size() == 2);
		boolean first = true;
		for (Person person : results) {
			assertNotNull(person);
			assertEquals(saved.getLastname(), person.getLastname());
			assertEquals(first ? saved.getFirstname() : saved2.getFirstname(), person.getFirstname());
			first = false;
		}
	}

	@Test
	public void testStringMethodResult() {

		Person saved = new Person();
		saved.setFirstname(uuid());
		saved.setLastname(uuid());
		saved.setNickname(uuid());

		saved = r.save(saved);

		String nickname = r.findSingleNickname(saved.getLastname(), saved.getFirstname());

		assertNotNull(nickname);
		assertEquals(saved.getNickname(), nickname);
	}

	@Test
	public void testBooleanMethodResult() {

		Person saved = new Person();
		saved.setFirstname(uuid());
		saved.setLastname(uuid());
		saved.setCool(true);

		saved = r.save(saved);

		boolean value = r.findSingleCool(saved.getLastname(), saved.getFirstname());

		assertEquals(saved.isCool(), value);
	}

	@Test
	public void testDateMethodResult() {

		Person saved = new Person();
		saved.setFirstname(uuid());
		saved.setLastname(uuid());
		saved.setBirthDate(new Date());

		saved = r.save(saved);

		Date value = r.findSingleBirthdate(saved.getLastname(), saved.getFirstname());

		assertEquals(saved.getBirthDate(), value);
	}

	@Test
	public void testIntMethodResult() {

		Person saved = new Person();
		saved.setFirstname(uuid());
		saved.setLastname(uuid());
		saved.setNumberOfChildren(1);

		saved = r.save(saved);

		int value = r.findSingleNumberOfChildren(saved.getLastname(), saved.getFirstname());

		assertEquals(saved.getNumberOfChildren(), value);
	}

	// TODO: @Test
	// public void testUuidMethodResult() {
	//
	// Person saved = new Person();
	// saved.setFirstname(uuid());
	// saved.setLastname(uuid());
	// saved.setUuid(UUID.randomUUID());
	//
	// saved = r.save(saved);
	//
	// UUID value = r.findSingleUuid(saved.getLastname(), saved.getFirstname());
	//
	// assertEquals(saved.getUuid(), value);
	// }

	@Test
	public void testArrayMethodSingleResult() {

		Person saved = new Person();
		saved.setFirstname(uuid());
		saved.setLastname(uuid());

		saved = r.save(saved);

		Person[] results = r.findFolksWithLastnameAsArray(saved.getLastname());

		assertNotNull(results);
		assertTrue(results.length == 1);
		Person found = results[0];
		assertNotNull(found);
		assertEquals(found.getLastname(), saved.getLastname());
		assertEquals(found.getFirstname(), saved.getFirstname());
	}

	@Test
	public void testEscapeSingleQuoteInQueryParameterValue() {

		Person saved = new Person();
		saved.setFirstname("Bri'an" + uuid());
		String lastname = "O'Brian" + uuid();
		saved.setLastname(lastname);

		saved = r.save(saved);

		List<Person> results = r.findFolksWithLastnameAsList(lastname);

		assertNotNull(results);
		assertTrue(results.size() == 1);
		for (Person person : results) {
			assertNotNull(person);
			assertEquals(saved.getLastname(), person.getLastname());
			assertEquals(saved.getFirstname(), person.getFirstname());
		}
	}
}
