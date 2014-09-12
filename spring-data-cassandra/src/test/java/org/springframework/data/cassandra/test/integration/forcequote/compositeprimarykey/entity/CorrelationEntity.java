package org.springframework.data.cassandra.test.integration.forcequote.compositeprimarykey.entity;

import org.springframework.cassandra.core.PrimaryKeyType;
import org.springframework.data.cassandra.mapping.*;

import java.io.Serializable;
import java.util.Date;
import java.util.Map;

@Table(value = "identity_correlations")
public class CorrelationEntity {

	@PrimaryKeyClass
	public static class IdentityEntity implements Serializable {

		private static final long serialVersionUID = 1027559675696864950L;

		@PrimaryKeyColumn(ordinal = 0, type = PrimaryKeyType.PARTITIONED)
		private String type;

		@PrimaryKeyColumn(ordinal = 1, type = PrimaryKeyType.PARTITIONED)
		private String value;

		@PrimaryKeyColumn(name = "correlated_type", ordinal = 2, type = PrimaryKeyType.CLUSTERED)
		private String correlatedType;

		@PrimaryKeyColumn(ordinal = 3, type = PrimaryKeyType.CLUSTERED)
		private Date ts;

		@PrimaryKeyColumn(name = "correlated_value", ordinal = 4, type = PrimaryKeyType.CLUSTERED)
		private String correlatedValue;

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

		public IdentityEntity(String type, String value, String correlatedType, Date ts, String correlatedValue) {
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
			if (!(o instanceof IdentityEntity))
				return false;

			IdentityEntity that = (IdentityEntity) o;

			if (correlatedType != null ? !correlatedType.equals(that.correlatedType) : that.correlatedType != null)
				return false;
			if (correlatedValue != null ? !correlatedValue.equals(that.correlatedValue) : that.correlatedValue != null)
				return false;
			if (ts != null ? !ts.equals(that.ts) : that.ts != null)
				return false;
			if (!type.equals(that.type))
				return false;
			if (!value.equals(that.value))
				return false;

			return true;
		}

		@Override
		public int hashCode() {
			int result = type.hashCode();
			result = 31 * result + value.hashCode();
			result = 31 * result + (correlatedType != null ? correlatedType.hashCode() : 0);
			result = 31 * result + (ts != null ? ts.hashCode() : 0);
			result = 31 * result + (correlatedValue != null ? correlatedValue.hashCode() : 0);
			return result;
		}
	}

	@PrimaryKey(forceQuote = true)
	private IdentityEntity identityEntity;

	@Column
	private Map<String, String> extra;

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
		if (!(o instanceof CorrelationEntity))
			return false;

		CorrelationEntity that = (CorrelationEntity) o;

		if (extra != null ? !extra.equals(that.extra) : that.extra != null)
			return false;
		if (!identityEntity.equals(that.identityEntity))
			return false;

		return true;
	}

	@Override
	public int hashCode() {
		int result = identityEntity.hashCode();
		result = 31 * result + (extra != null ? extra.hashCode() : 0);
		return result;
	}
}
