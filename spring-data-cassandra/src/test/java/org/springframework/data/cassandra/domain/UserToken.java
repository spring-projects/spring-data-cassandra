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

import java.util.UUID;

import org.springframework.data.cassandra.core.cql.PrimaryKeyType;
import org.springframework.data.cassandra.core.mapping.Column;
import org.springframework.data.cassandra.core.mapping.PrimaryKeyColumn;
import org.springframework.data.cassandra.core.mapping.Table;
import org.springframework.util.ObjectUtils;

/**
 * @author Mark Paluch
 */
@Table("user_tokens")
public class UserToken {

	@PrimaryKeyColumn(name = "user_id", type = PrimaryKeyType.PARTITIONED, ordinal = 0) private UUID userId;
	@PrimaryKeyColumn(name = "auth_token", type = PrimaryKeyType.CLUSTERED, ordinal = 1) private UUID token;

	@Column("user_comment") String userComment;
	String adminComment;

	public UUID getUserId() {
		return userId;
	}

	public void setUserId(UUID userId) {
		this.userId = userId;
	}

	public UUID getToken() {
		return token;
	}

	public void setToken(UUID token) {
		this.token = token;
	}

	public String getUserComment() {
		return userComment;
	}

	public void setUserComment(String userComment) {
		this.userComment = userComment;
	}

	public String getAdminComment() {
		return adminComment;
	}

	public void setAdminComment(String adminComment) {
		this.adminComment = adminComment;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;

		UserToken userToken = (UserToken) o;

		if (!ObjectUtils.nullSafeEquals(userId, userToken.userId)) {
			return false;
		}
		if (!ObjectUtils.nullSafeEquals(token, userToken.token)) {
			return false;
		}
		if (!ObjectUtils.nullSafeEquals(userComment, userToken.userComment)) {
			return false;
		}
		return ObjectUtils.nullSafeEquals(adminComment, userToken.adminComment);
	}

	@Override
	public int hashCode() {
		int result = ObjectUtils.nullSafeHashCode(userId);
		result = 31 * result + ObjectUtils.nullSafeHashCode(token);
		result = 31 * result + ObjectUtils.nullSafeHashCode(userComment);
		result = 31 * result + ObjectUtils.nullSafeHashCode(adminComment);
		return result;
	}
}
