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
package org.springframework.data.cassandra.domain;

import lombok.Data;

import java.util.UUID;

import org.springframework.cassandra.core.PrimaryKeyType;
import org.springframework.data.cassandra.mapping.CassandraType;
import org.springframework.data.cassandra.mapping.Column;
import org.springframework.data.cassandra.mapping.PrimaryKeyColumn;
import org.springframework.data.cassandra.mapping.Table;

import com.datastax.driver.core.DataType.Name;

/**
 * @author Mark Paluch
 */
@Table("user_tokens")
@Data
public class UserToken {

	@PrimaryKeyColumn(name = "user_id", type = PrimaryKeyType.PARTITIONED,
			ordinal = 0) @CassandraType(type = Name.UUID) private UUID userId;
	@PrimaryKeyColumn(name = "auth_token", type = PrimaryKeyType.CLUSTERED,
			ordinal = 1) @CassandraType(type = Name.UUID) private UUID token;

	@Column("user_comment") String userComment;
	String adminComment;
}
