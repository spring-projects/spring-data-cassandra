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
package org.springframework.data.cassandra.test.integration.simpletons;

import java.util.Date;
import java.util.List;
import java.util.Set;

import org.springframework.data.cassandra.mapping.PrimaryKey;
import org.springframework.data.cassandra.mapping.Table;

/**
 * Test POJO
 * 
 * @author David Webb
 */
@Table("bookReference")
public class BookReference {

	@PrimaryKey
	private String isbn;

	private String title;
	private String author;
	private int pages;
	private Date saleDate;
	private boolean isInStock;
	private Set<String> references;
	private List<Integer> bookmarks;

	/**
	 * @return Returns the isbn.
	 */
	public String getIsbn() {
		return isbn;
	}

	/**
	 * @return Returns the saleDate.
	 */
	public Date getSaleDate() {
		return saleDate;
	}

	/**
	 * @param saleDate The saleDate to set.
	 */
	public void setSaleDate(Date saleDate) {
		this.saleDate = saleDate;
	}

	/**
	 * @return Returns the isInStock.
	 */
	public boolean isInStock() {
		return isInStock;
	}

	/**
	 * @param isInStock The isInStock to set.
	 */
	public void setInStock(boolean isInStock) {
		this.isInStock = isInStock;
	}

	/**
	 * @param isbn The isbn to set.
	 */
	public void setIsbn(String isbn) {
		this.isbn = isbn;
	}

	/**
	 * @return Returns the title.
	 */
	public String getTitle() {
		return title;
	}

	/**
	 * @param title The title to set.
	 */
	public void setTitle(String title) {
		this.title = title;
	}

	/**
	 * @return Returns the author.
	 */
	public String getAuthor() {
		return author;
	}

	/**
	 * @param author The author to set.
	 */
	public void setAuthor(String author) {
		this.author = author;
	}

	/**
	 * @return Returns the pages.
	 */
	public int getPages() {
		return pages;
	}

	/**
	 * @param pages The pages to set.
	 */
	public void setPages(int pages) {
		this.pages = pages;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("isbn -> " + isbn).append("\n");
		sb.append("tile -> " + title).append("\n");
		sb.append("author -> " + author).append("\n");
		sb.append("pages -> " + pages).append("\n");
		return sb.toString();
	}

	/**
	 * @return Returns the references.
	 */
	public Set<String> getReferences() {
		return references;
	}

	/**
	 * @param references The references to set.
	 */
	public void setReferences(Set<String> references) {
		this.references = references;
	}

	/**
	 * @return Returns the bookmarks.
	 */
	public List<Integer> getBookmarks() {
		return bookmarks;
	}

	/**
	 * @param bookmarks The bookmarks to set.
	 */
	public void setBookmarks(List<Integer> bookmarks) {
		this.bookmarks = bookmarks;
	}

}
