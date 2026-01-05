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
package org.springframework.data.cassandra.repository.forcequote.compositeprimarykey.entity;

import java.io.Serializable;
import java.util.Date;
import java.util.Map;

import org.springframework.data.cassandra.core.cql.PrimaryKeyType;
import org.springframework.data.cassandra.core.mapping.Column;
import org.springframework.data.cassandra.core.mapping.PrimaryKey;
import org.springframework.data.cassandra.core.mapping.PrimaryKeyClass;
import org.springframework.data.cassandra.core.mapping.PrimaryKeyColumn;
import org.springframework.data.cassandra.core.mapping.Table;
import org.springframework.util.ObjectUtils;

/**
 * @author Matthew T. Adams
 */
@Table(value = "identity_correlations")
public class CorrelationEntity {

	@PrimaryKeyClass
	public static class IdentityEntity implements Serializable {

		private static final long serialVersionUID = 1027559675696864950L;

		@PrimaryKeyColumn(ordinal = 0, type = PrimaryKeyType.PARTITIONED) private String type;

		@PrimaryKeyColumn(ordinal = 1, type = PrimaryKeyType.PARTITIONED) private String value;

		@PrimaryKeyColumn(name = "correlated_type", ordinal = 2,
				type = PrimaryKeyType.CLUSTERED) private String correlatedType;

		@PrimaryKeyColumn(ordinal = 3, type = PrimaryKeyType.CLUSTERED) private Date ts;

		@PrimaryKeyColumn(name = "correlated_value", ordinal = 4,
				type = PrimaryKeyType.CLUSTERED) private String correlatedValue;

		public IdentityEntity() {}

		public IdentityEntity(String type, String value) {
			this.type = type;
			this.value = value;
		}

		public IdentityEntity(String type, String value, String correlatedType) {
			this.type = type;
			this.value = value;
			this.correlatedType = correlatedType;
		}

		public IdentityEntity(String type, String value, String correlatedType, Date ts) {
			this.type = type;
			this.value = value;
			this.correlatedType = correlatedType;
			this.ts = ts;
		}

		private IdentityEntity(String type, String value, String correlatedType, Date ts, String correlatedValue) {
			this.type = type;
			this.value = value;
			this.correlatedType = correlatedType;
			this.ts = ts;
			this.correlatedValue = correlatedValue;
		}

		public String getType() {
			return type;
		}

		public void setType(String type) {
			this.type = type;
		}

		public String getValue() {
			return value;
		}

		public void setValue(String value) {
			this.value = value;
		}

		public String getCorrelatedType() {
			return correlatedType;
		}

		public void setCorrelatedType(String correlatedType) {
			this.correlatedType = correlatedType;
		}

		public Date getTs() {
			return ts;
		}

		public void setTs(Date ts) {
			this.ts = ts;
		}

		public String getCorrelatedValue() {
			return correlatedValue;
		}

		public void setCorrelatedValue(String correlatedValue) {
			this.correlatedValue = correlatedValue;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o)
				return true;
			if (o == null || getClass() != o.getClass())
				return false;

			IdentityEntity that = (IdentityEntity) o;

			if (!ObjectUtils.nullSafeEquals(type, that.type)) {
				return false;
			}
			if (!ObjectUtils.nullSafeEquals(value, that.value)) {
				return false;
			}
			if (!ObjectUtils.nullSafeEquals(correlatedType, that.correlatedType)) {
				return false;
			}
			if (!ObjectUtils.nullSafeEquals(ts, that.ts)) {
				return false;
			}
			return ObjectUtils.nullSafeEquals(correlatedValue, that.correlatedValue);
		}

		@Override
		public int hashCode() {
			int result = ObjectUtils.nullSafeHashCode(type);
			result = 31 * result + ObjectUtils.nullSafeHashCode(value);
			result = 31 * result + ObjectUtils.nullSafeHashCode(correlatedType);
			result = 31 * result + ObjectUtils.nullSafeHashCode(ts);
			result = 31 * result + ObjectUtils.nullSafeHashCode(correlatedValue);
			return result;
		}
	}

	@PrimaryKey(forceQuote = true) private IdentityEntity identityEntity;

	@Column private Map<String, String> extra;

	public CorrelationEntity() {}

	public CorrelationEntity(IdentityEntity identityEntity, Map<String, String> extra) {
		this.identityEntity = identityEntity;
		this.extra = extra;
	}

	public CorrelationEntity(String type, String value, String correlatedType, Date ts, String correlatedValue,
			Map<String, String> extra) {
		this.identityEntity = new IdentityEntity(type, value, correlatedType, ts, correlatedValue);
		this.extra = extra;
	}

	public IdentityEntity getIdentityEntity() {
		return identityEntity;
	}

	public void setIdentityEntity(IdentityEntity identityEntity) {
		this.identityEntity = identityEntity;
	}

	public Map<String, String> getExtra() {
		return extra;
	}

	public void setExtra(Map<String, String> extra) {
		this.extra = extra;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;

		CorrelationEntity that = (CorrelationEntity) o;

		if (!ObjectUtils.nullSafeEquals(identityEntity, that.identityEntity)) {
			return false;
		}
		return ObjectUtils.nullSafeEquals(extra, that.extra);
	}

	@Override
	public int hashCode() {
		int result = ObjectUtils.nullSafeHashCode(identityEntity);
		result = 31 * result + ObjectUtils.nullSafeHashCode(extra);
		return result;
	}
}
