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
package org.springframework.data.cassandra.repository.conversion;

import static org.assertj.core.api.Assertions.*;

import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.cassandra.repository.CassandraRepository;
import org.springframework.data.cassandra.repository.Query;
import org.springframework.data.cassandra.test.integration.repository.querymethods.declared.base.PersonRepository;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.UDTValue;

/**
 * Integration tests for query argument conversion through {@link PersonRepository}.
 *
 * @author Mark Paluch
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = ParameterConversionTestSupport.Config.class)
public class StringQueryMethodsParameterConversionIntegrationTests extends ParameterConversionTestSupport {

	@Autowired ContactStringQueryRepository contactRepository;

	/**
	 * @see DATACASS-172
	 */
	@Test
	public void shouldFindByConvertedParameter() {

		List<Contact> contacts = contactRepository.findByAddress(walter.getAddress());

		assertThat(contacts).contains(walter, flynn);
	}

	/**
	 * @see DATACASS-172
	 */
	@Test
	public void shouldFindByStringParameter() {

		String parameter = AddressWriteConverter.INSTANCE.convert(walter.getAddress());
		List<Contact> contacts = contactRepository.findByAddress(parameter);

		assertThat(contacts).contains(walter, flynn);
	}

	/**
	 * @see DATACASS-172
	 */
	@Test
	public void findByAddressesIn() {

		assertThat(contactRepository.findByAddressesContains(flynn.address)).contains(flynn, walter);
		assertThat(contactRepository.findByAddressesContains(walter.addresses.get(1))).contains(walter);
	}

	/**
	 * @see DATACASS-172
	 */
	@Test
	public void findByMainPhone() {
		assertThat(contactRepository.findByMainPhone(walter.getMainPhone())).contains(walter);
	}

	/**
	 * @see DATACASS-172
	 */
	@Test
	public void findByMainPhoneUdtValue() {

		KeyspaceMetadata keyspace = adminOperations.getKeyspaceMetadata();
		UDTValue udtValue = keyspace.getUserType("phone").newValue();
		udtValue.setString("number", walter.getMainPhone().getNumber());

		assertThat(contactRepository.findByMainPhone(udtValue)).contains(walter);
	}

	/**
	 * @see DATACASS-172
	 */
	@Test
	public void findByAlternativePhones() {

		Phone phone = walter.getAlternativePhones().get(0);
		assertThat(contactRepository.findByAlternativePhonesContains(phone)).contains(walter);
	}

	/**
	 * @see DATACASS-172
	 */
	@Test
	public void findByAlternativePhonesUdtValue() {

		Phone phone = walter.getAlternativePhones().get(0);

		KeyspaceMetadata keyspace = adminOperations.getKeyspaceMetadata();
		UDTValue udtValue = keyspace.getUserType("phone").newValue();
		udtValue.setString("number", phone.getNumber());

		assertThat(contactRepository.findByAlternativePhonesContains(udtValue)).contains(walter);
	}

	interface ContactStringQueryRepository extends CassandraRepository<Contact> {

		@Query("SELECT * from contact where address = ?0;")
		List<Contact> findByAddress(Address address);

		@Query("SELECT * from contact where address = ?0;")
		List<Contact> findByAddress(String address);

		@Query("SELECT * from contact where addresses contains ?0;")
		List<Contact> findByAddressesContains(Address address);

		@Query("SELECT * from contact where mainphone = ?0;")
		List<Contact> findByMainPhone(Phone phone);

		@Query("SELECT * from contact where mainphone = ?0;")
		List<Contact> findByMainPhone(UDTValue udtValue);

		@Query("SELECT * from contact where alternativephones contains ?0;")
		List<Contact> findByAlternativePhonesContains(Phone phone);

		@Query("SELECT * from contact where alternativephones contains ?0;")
		List<Contact> findByAlternativePhonesContains(UDTValue udtValue);
	}
}
