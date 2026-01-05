/*
 * Copyright 2016-present the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.domain;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Date;
import java.util.List;

import org.springframework.data.cassandra.core.cql.PrimaryKeyType;
import org.springframework.data.cassandra.core.mapping.PrimaryKeyColumn;
import org.springframework.data.cassandra.core.mapping.Table;
import org.springframework.util.ObjectUtils;

/**
 * @author Mark Paluch
 */
@Table
public class Person {

	@PrimaryKeyColumn(type = PrimaryKeyType.PARTITIONED, ordinal = 0) private String lastname;

	@PrimaryKeyColumn(type = PrimaryKeyType.CLUSTERED, ordinal = 1) private String firstname;

	private String nickname;
	private Date birthDate;
	private int numberOfChildren;
	private boolean cool;

	private LocalDate createdDate;
	private ZoneId zoneId;

	private AddressType mainAddress;
	private List<AddressType> alternativeAddresses;

	public Person(String firstname, String lastname) {

		this.firstname = firstname;
		this.lastname = lastname;
	}

	public Person(String lastname, String firstname, String nickname, Date birthDate, int numberOfChildren, boolean cool,
			LocalDate createdDate, ZoneId zoneId, AddressType mainAddress, List<AddressType> alternativeAddresses) {
		this.lastname = lastname;
		this.firstname = firstname;
		this.nickname = nickname;
		this.birthDate = birthDate;
		this.numberOfChildren = numberOfChildren;
		this.cool = cool;
		this.createdDate = createdDate;
		this.zoneId = zoneId;
		this.mainAddress = mainAddress;
		this.alternativeAddresses = alternativeAddresses;
	}

	public Person() {}

	public String getLastname() {
		return this.lastname;
	}

	public String getFirstname() {
		return this.firstname;
	}

	public String getNickname() {
		return this.nickname;
	}

	public Date getBirthDate() {
		return this.birthDate;
	}

	public int getNumberOfChildren() {
		return this.numberOfChildren;
	}

	public boolean isCool() {
		return this.cool;
	}

	public LocalDate getCreatedDate() {
		return this.createdDate;
	}

	public ZoneId getZoneId() {
		return this.zoneId;
	}

	public AddressType getMainAddress() {
		return this.mainAddress;
	}

	public List<AddressType> getAlternativeAddresses() {
		return this.alternativeAddresses;
	}

	public void setLastname(String lastname) {
		this.lastname = lastname;
	}

	public void setFirstname(String firstname) {
		this.firstname = firstname;
	}

	public void setNickname(String nickname) {
		this.nickname = nickname;
	}

	public void setBirthDate(Date birthDate) {
		this.birthDate = birthDate;
	}

	public void setNumberOfChildren(int numberOfChildren) {
		this.numberOfChildren = numberOfChildren;
	}

	public void setCool(boolean cool) {
		this.cool = cool;
	}

	public void setCreatedDate(LocalDate createdDate) {
		this.createdDate = createdDate;
	}

	public void setZoneId(ZoneId zoneId) {
		this.zoneId = zoneId;
	}

	public void setMainAddress(AddressType mainAddress) {
		this.mainAddress = mainAddress;
	}

	public void setAlternativeAddresses(List<AddressType> alternativeAddresses) {
		this.alternativeAddresses = alternativeAddresses;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;

		Person person = (Person) o;

		if (numberOfChildren != person.numberOfChildren)
			return false;
		if (cool != person.cool)
			return false;
		if (!ObjectUtils.nullSafeEquals(lastname, person.lastname)) {
			return false;
		}
		if (!ObjectUtils.nullSafeEquals(firstname, person.firstname)) {
			return false;
		}
		if (!ObjectUtils.nullSafeEquals(nickname, person.nickname)) {
			return false;
		}
		if (!ObjectUtils.nullSafeEquals(birthDate, person.birthDate)) {
			return false;
		}
		if (!ObjectUtils.nullSafeEquals(createdDate, person.createdDate)) {
			return false;
		}
		if (!ObjectUtils.nullSafeEquals(zoneId, person.zoneId)) {
			return false;
		}
		if (!ObjectUtils.nullSafeEquals(mainAddress, person.mainAddress)) {
			return false;
		}
		return ObjectUtils.nullSafeEquals(alternativeAddresses, person.alternativeAddresses);
	}

	@Override
	public int hashCode() {
		int result = ObjectUtils.nullSafeHashCode(lastname);
		result = 31 * result + ObjectUtils.nullSafeHashCode(firstname);
		result = 31 * result + ObjectUtils.nullSafeHashCode(nickname);
		result = 31 * result + ObjectUtils.nullSafeHashCode(birthDate);
		result = 31 * result + numberOfChildren;
		result = 31 * result + (cool ? 1 : 0);
		result = 31 * result + ObjectUtils.nullSafeHashCode(createdDate);
		result = 31 * result + ObjectUtils.nullSafeHashCode(zoneId);
		result = 31 * result + ObjectUtils.nullSafeHashCode(mainAddress);
		result = 31 * result + ObjectUtils.nullSafeHashCode(alternativeAddresses);
		return result;
	}
}
