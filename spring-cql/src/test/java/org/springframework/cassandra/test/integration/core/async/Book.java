/*
 * Copyright 2016-2017 the original author or authors.
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
package org.springframework.cassandra.test.integration.core.async;

import java.util.UUID;

/**
 * @author Matthew T. Adams
 */
public class Book {

	public static final String uuid() {
		return UUID.randomUUID().toString();
	}

	public static Book random() {
		return new Book("title-" + uuid(), "isbn-" + uuid());
	}

	public Book() {}

	public Book(String title, String isbn) {
		this.isbn = isbn;
		this.title = title;
	}

	public String isbn;
	public String title;
}
