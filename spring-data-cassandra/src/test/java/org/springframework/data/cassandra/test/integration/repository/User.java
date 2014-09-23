/*
 * Copyright 2013-2014 the original author or authors
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.test.integration.repository;

import java.util.Set;

import org.springframework.data.annotation.Id;
import org.springframework.data.cassandra.mapping.Indexed;
import org.springframework.data.cassandra.mapping.Table;

/**
 * This is an example of the Users status table, where all fields are columns in Cassandra row. Some fields can be
 * Set,List,Map like emails. User contains base information related for separate user, like names, additional
 * information, emails, following users, friends.
 * 
 * @author Alex Shvid
 */
@Table("users")
public class User {

	/*
	 * Primary Row ID
	 */
	@Id
	private String username;

	/*
	 * Public information
	 */
	private String firstName;
	private String lastName;

	/*
	 * Secondary index, used only on fields with common information,
	 * not effective on email, username
	 */
	@Indexed
	private String place;

	/*
	 * User emails
	 */
	private Set<String> emails;

	/*
	 * Password
	 */
	private String password;

	/*
	 * Birth Year 
	 */
	private int birthYear;

	/*
	 * Following other users in userline
	 */
	private Set<String> following;

	/*
	 * Friends of the user
	 */
	private Set<String> friends;

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getFirstName() {
		return firstName;
	}

	public void setFirstName(String firstName) {
		this.firstName = firstName;
	}

	public String getLastName() {
		return lastName;
	}

	public void setLastName(String lastName) {
		this.lastName = lastName;
	}

	public String getPlace() {
		return place;
	}

	public void setPlace(String place) {
		this.place = place;
	}

	public Set<String> getEmails() {
		return emails;
	}

	public void setEmails(Set<String> emails) {
		this.emails = emails;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public Set<String> getFollowing() {
		return following;
	}

	public void setFollowing(Set<String> following) {
		this.following = following;
	}

	public Set<String> getFriends() {
		return friends;
	}

	public void setFriends(Set<String> friends) {
		this.friends = friends;
	}

	/**
	 * @return Returns the birthYear.
	 */
	public int getBirthYear() {
		return birthYear;
	}

	/**
	 * @param birthYear The birthYear to set.
	 */
	public void setBirthYear(int birthYear) {
		this.birthYear = birthYear;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((username == null) ? 0 : username.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		User other = (User) obj;
		if (username == null) {
			if (other.username != null)
				return false;
		} else if (!username.equals(other.username))
			return false;
		return true;
	}

}
