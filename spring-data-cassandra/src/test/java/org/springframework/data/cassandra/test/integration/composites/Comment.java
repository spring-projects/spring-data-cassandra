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
package org.springframework.data.cassandra.test.integration.composites;

import org.springframework.data.cassandra.mapping.PrimaryKey;
import org.springframework.data.cassandra.mapping.Table;
import org.springframework.util.Assert;

/**
 * This is an example of dynamic table (wide row). PartitionKey (former RowId) is pk.author. ClusteredColumn (former
 * Column Id) is pk.time
 * 
 * @author Alex Shvid
 * @author Matthew T. Adams
 */
@Table("comments")
public class Comment {

	@PrimaryKey
	private CommentKey pk;

	private String text;

	/**
	 * @deprecated Only for use by persistence infrastructure
	 */
	@Deprecated
	protected Comment() {}

	public Comment(String author, String company) {
		this(new CommentKey(author, company));
	}

	public Comment(CommentKey pk) {
		Assert.notNull(pk);
		this.pk = pk;
	}

	public CommentKey getId() {
		return pk;
	}

	public void setPk(CommentKey pk) {
		this.pk = pk;
	}

	public String getText() {
		return text;
	}

	public void setText(String text) {
		this.text = text;
	}

	@Override
	public boolean equals(Object that) {

		if (this == that) {
			return true;
		}
		if (that == null) {
			return false;
		}
		if (!(that instanceof Comment)) {
			return false;
		}

		Comment other = (Comment) that;

		if (this.pk == null) {
			return other.pk == null;
		}

		return this.pk.equals(other.pk);
	}

	@Override
	public int hashCode() {
		return pk.hashCode();
	}
}
