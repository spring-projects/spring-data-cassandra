/*
 * Copyright 2016 the original author or authors.
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
package org.springframework.data.cassandra.test.integration.repository.querymethods.conversion;

import static org.assertj.core.api.Assertions.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.cassandra.config.SchemaAction;
import org.springframework.data.cassandra.convert.CustomConversions;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.repository.CassandraRepository;
import org.springframework.data.cassandra.repository.config.EnableCassandraRepositories;
import org.springframework.data.cassandra.test.integration.repository.querymethods.declared.base.PersonRepository;
import org.springframework.data.cassandra.test.integration.support.AbstractSpringDataEmbeddedCassandraIntegrationTest;
import org.springframework.data.cassandra.test.integration.support.IntegrationTestConfig;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.util.StringUtils;

/**
 * Integration tests for query derivation through {@link PersonRepository}.
 *
 * @author Mark Paluch
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class ParameterConversionIntegrationTests extends AbstractSpringDataEmbeddedCassandraIntegrationTest {

	@Configuration
	@EnableCassandraRepositories(considerNestedRepositories = true)
	public static class Config extends IntegrationTestConfig {

		@Override
		public String[] getEntityBasePackages() {
			return new String[] { Contact.class.getPackage().getName() };
		}

		@Override
		public SchemaAction getSchemaAction() {
			return SchemaAction.RECREATE_DROP_UNUSED;
		}

		@Override
		public CustomConversions customConversions() {
			return new CustomConversions(Arrays.asList(AddressReadConverter.INSTANCE, AddressWriteConverter.INSTANCE));
		}
	}

	@Autowired CassandraOperations template;
	@Autowired ContactRepository contactRepository;

	Contact walter, flynn;

	@Before
	public void before() {

		deleteAllEntities();

		template.execute("CREATE INDEX IF NOT EXISTS contact_address ON contact (address);");
		template.execute("CREATE INDEX IF NOT EXISTS contact_addresses ON contact (addresses);");

		walter = new Contact("Walter");
		walter.setAddress(new Address("Albuquerque", "USA"));
		walter.setAddresses(Arrays.asList(new Address("Albuquerque", "USA"), new Address("New Hampshire", "USA"),
				new Address("Grocery Store", "Mexico")));

		flynn = new Contact("Flynn");
		flynn.setAddress(new Address("Albuquerque", "USA"));
		flynn.setAddresses(Collections.singletonList(new Address("Albuquerque", "USA")));

		walter = contactRepository.save(walter);
		flynn = contactRepository.save(flynn);
	}

	/**
	 * @see DATACASS-7
	 */
	@Test
	public void shouldFindByConvertedParameter() {

		List<Contact> contacts = contactRepository.findByAddress(walter.getAddress());

		assertThat(contacts).contains(walter, flynn);
	}

	/**
	 * @see DATACASS-7
	 */
	@Test
	public void shouldFindByStringParameter() {

		String parameter = AddressWriteConverter.INSTANCE.convert(walter.getAddress());
		List<Contact> contacts = contactRepository.findByAddress(parameter);

		assertThat(contacts).contains(walter, flynn);
	}

	/**
	 * @see DATACASS-7
	 */
	@Test
	public void findByAddressesIn() {

		assertThat(contactRepository.findByAddressesContains(flynn.address)).contains(flynn, walter);
		assertThat(contactRepository.findByAddressesContains(walter.addresses.get(1))).contains(walter);
	}

	interface ContactRepository extends CassandraRepository<Contact> {

		List<Contact> findByAddress(Address address);

		List<Contact> findByAddress(String address);

		List<Contact> findByAddressesContains(Address address);
	}

	/**
	 * @author Mark Paluch
	 */
	static enum AddressReadConverter implements Converter<String, Address> {

		INSTANCE;

		public Address convert(String source) {

			if (StringUtils.hasText(source)) {
				try {
					return new ObjectMapper().readValue(source, Address.class);
				} catch (IOException e) {
					throw new IllegalStateException(e);
				}
			}

			return null;
		}
	}

	/**
	 * @author Mark Paluch
	 */
	static enum AddressWriteConverter implements Converter<Address, String> {
		INSTANCE;

		public String convert(Address source) {

			try {
				return new ObjectMapper().writeValueAsString(source);
			} catch (IOException e) {
				throw new IllegalStateException(e);
			}
		}
	}
}
