package org.springframework.cassandra.test.integration.core.template.async;

import java.util.UUID;

public class Book {

	public static final String uuid() {
		return UUID.randomUUID().toString();
	}

	public static Book random() {

		String uuid = uuid();
		return new Book("title-" + uuid, "isbn-" + uuid);
	}

	public Book() {}

	public Book(String title, String isbn) {
		this.isbn = isbn;
		this.title = title;
	}

	public String isbn;
	public String title;
}
