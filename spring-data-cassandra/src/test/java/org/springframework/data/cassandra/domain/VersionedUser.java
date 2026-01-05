/*
 * Copyright 2019-present the original author or authors.
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

import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.PersistenceCreator;
import org.springframework.data.annotation.Version;
import org.springframework.data.cassandra.core.mapping.Table;
import org.springframework.util.ObjectUtils;

/**
 * @author Mark Paluch
 */
@Table("vusers")
public class VersionedUser {

	/*
	 * Primary Row ID
	 */
	@Id private String id;

	@Version private Long version;

	/*
	 * Public information
	 */
	private String firstname;
	private String lastname;

	@PersistenceCreator
	public VersionedUser(String id, String firstname, String lastname) {

		this.id = id;
		this.firstname = firstname;
		this.lastname = lastname;
	}

	public VersionedUser() {}

	public String getId() {
		return this.id;
	}

	public Long getVersion() {
		return this.version;
	}

	public String getFirstname() {
		return this.firstname;
	}

	public String getLastname() {
		return this.lastname;
	}

	public void setId(String id) {
		this.id = id;
	}

	public void setVersion(Long version) {
		this.version = version;
	}

	public void setFirstname(String firstname) {
		this.firstname = firstname;
	}

	public void setLastname(String lastname) {
		this.lastname = lastname;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;

		VersionedUser that = (VersionedUser) o;

		if (!ObjectUtils.nullSafeEquals(id, that.id)) {
			return false;
		}
		if (!ObjectUtils.nullSafeEquals(version, that.version)) {
			return false;
		}
		if (!ObjectUtils.nullSafeEquals(firstname, that.firstname)) {
			return false;
		}
		return ObjectUtils.nullSafeEquals(lastname, that.lastname);
	}

	@Override
	public int hashCode() {
		int result = ObjectUtils.nullSafeHashCode(id);
		result = 31 * result + ObjectUtils.nullSafeHashCode(version);
		result = 31 * result + ObjectUtils.nullSafeHashCode(firstname);
		result = 31 * result + ObjectUtils.nullSafeHashCode(lastname);
		return result;
	}
}
