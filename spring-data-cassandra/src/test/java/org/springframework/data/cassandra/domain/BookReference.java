/*
 * Copyright 2017-2025 the original author or authors.
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


import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.data.cassandra.core.mapping.PrimaryKey;
import org.springframework.data.cassandra.core.mapping.Table;

/**
 * Test POJO
 *
 * @author David Webb
 * @author Mark Paluch
 */
@Table("bookReference")
public class BookReference {

	@PrimaryKey private String isbn;

	private String title;
	private Set<String> references;
	private List<Integer> bookmarks;
	private Map<String, String> credits;

	public String getIsbn() {
		return this.isbn;
	}

	public String getTitle() {
		return this.title;
	}

	public Set<String> getReferences() {
		return this.references;
	}

	public List<Integer> getBookmarks() {
		return this.bookmarks;
	}

	public Map<String, String> getCredits() {
		return this.credits;
	}

	public void setIsbn(String isbn) {
		this.isbn = isbn;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public void setReferences(Set<String> references) {
		this.references = references;
	}

	public void setBookmarks(List<Integer> bookmarks) {
		this.bookmarks = bookmarks;
	}

	public void setCredits(Map<String, String> credits) {
		this.credits = credits;
	}
}
