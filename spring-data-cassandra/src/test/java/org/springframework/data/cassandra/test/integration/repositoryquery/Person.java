/*
 * Copyright 2010-2013 the original author or authors.
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
package org.springframework.data.cassandra.test.integration.repositoryquery;

import java.util.Date;

import org.springframework.cassandra.core.PrimaryKeyType;
import org.springframework.data.cassandra.mapping.PrimaryKeyColumn;
import org.springframework.data.cassandra.mapping.Table;

/**
 * Sample domain class.
 */
@Table
public class Person {

	@PrimaryKeyColumn(type = PrimaryKeyType.PARTITIONED, ordinal = 0)
	private String lastname;

	@PrimaryKeyColumn(type = PrimaryKeyType.CLUSTERED, ordinal = 1)
	private String firstname;

	private String nickname;

	private Date birthDate;

	private int numberOfChildren;

	private boolean cool;

	// TODO: private UUID uuid = UUID.randomUUID();

	public String getFirstname() {
		return firstname;
	}

	public void setFirstname(String firstname) {
		this.firstname = firstname;
	}

	public String getLastname() {
		return lastname;
	}

	public void setLastname(String lastname) {
		this.lastname = lastname;
	}

	public String getNickname() {
		return nickname;
	}

	public void setNickname(String nickname) {
		this.nickname = nickname;
	}

	public Date getBirthDate() {
		return new Date(birthDate.getTime());
	}

	public void setBirthDate(Date birthDate) {
		this.birthDate = birthDate == null ? null : new Date(birthDate.getTime());
	}

	public int getNumberOfChildren() {
		return numberOfChildren;
	}

	public void setNumberOfChildren(int numberOfChildren) {
		this.numberOfChildren = numberOfChildren;
	}

	public boolean isCool() {
		return cool;
	}

	public void setCool(boolean cool) {
		this.cool = cool;
	}

	// public UUID getUuid() {
	// return uuid;
	// }
	//
	// public void setUuid(UUID uuid) {
	// this.uuid = uuid;
	// }
}
