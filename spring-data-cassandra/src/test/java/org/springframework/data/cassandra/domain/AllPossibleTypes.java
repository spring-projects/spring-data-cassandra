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

import static org.springframework.data.cassandra.core.mapping.CassandraType.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.springframework.data.cassandra.core.convert.CassandraTypeMappingIntegrationTests.Condition;
import org.springframework.data.cassandra.core.mapping.CassandraType;
import org.springframework.data.cassandra.core.mapping.PrimaryKey;
import org.springframework.data.cassandra.core.mapping.Table;
import org.springframework.util.ObjectUtils;

import com.datastax.oss.driver.api.core.data.TupleValue;

/**
 * @author Mark Paluch
 */
@Table
public class AllPossibleTypes {

	@PrimaryKey String id;

	InetAddress inet;

	UUID uuid;

	@CassandraType(type = Name.INT) Number justNumber;

	Byte boxedByte;
	byte primitiveByte;

	Short boxedShort;
	short primitiveShort;

	Long boxedLong;
	long primitiveLong;

	Integer boxedInteger;
	int primitiveInteger;

	Float boxedFloat;
	float primitiveFloat;

	Double boxedDouble;
	double primitiveDouble;

	Boolean boxedBoolean;
	boolean primitiveBoolean;

	java.time.Instant instant;
	java.time.LocalDate date;
	java.time.LocalTime time;

	Date timestamp;

	BigDecimal bigDecimal;
	BigInteger bigInteger;
	ByteBuffer blob;

	Set<String> setOfString;
	List<String> listOfString;
	Map<String, String> mapOfString;

	Condition anEnum;
	Set<Condition> setOfEnum;
	List<Condition> listOfEnum;

	@CassandraType(type = Name.TUPLE, typeArguments = { Name.VARCHAR, Name.BIGINT }) TupleValue tupleValue;

	// supported by conversion
	java.time.LocalDateTime localDateTime;
	java.time.ZoneId zoneId;

	public AllPossibleTypes(String id) {
		this.id = id;
	}

	public AllPossibleTypes() {}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public InetAddress getInet() {
		return inet;
	}

	public void setInet(InetAddress inet) {
		this.inet = inet;
	}

	public UUID getUuid() {
		return uuid;
	}

	public void setUuid(UUID uuid) {
		this.uuid = uuid;
	}

	public Number getJustNumber() {
		return justNumber;
	}

	public void setJustNumber(Number justNumber) {
		this.justNumber = justNumber;
	}

	public Byte getBoxedByte() {
		return boxedByte;
	}

	public void setBoxedByte(Byte boxedByte) {
		this.boxedByte = boxedByte;
	}

	public byte getPrimitiveByte() {
		return primitiveByte;
	}

	public void setPrimitiveByte(byte primitiveByte) {
		this.primitiveByte = primitiveByte;
	}

	public Short getBoxedShort() {
		return boxedShort;
	}

	public void setBoxedShort(Short boxedShort) {
		this.boxedShort = boxedShort;
	}

	public short getPrimitiveShort() {
		return primitiveShort;
	}

	public void setPrimitiveShort(short primitiveShort) {
		this.primitiveShort = primitiveShort;
	}

	public Long getBoxedLong() {
		return boxedLong;
	}

	public void setBoxedLong(Long boxedLong) {
		this.boxedLong = boxedLong;
	}

	public long getPrimitiveLong() {
		return primitiveLong;
	}

	public void setPrimitiveLong(long primitiveLong) {
		this.primitiveLong = primitiveLong;
	}

	public Integer getBoxedInteger() {
		return boxedInteger;
	}

	public void setBoxedInteger(Integer boxedInteger) {
		this.boxedInteger = boxedInteger;
	}

	public int getPrimitiveInteger() {
		return primitiveInteger;
	}

	public void setPrimitiveInteger(int primitiveInteger) {
		this.primitiveInteger = primitiveInteger;
	}

	public Float getBoxedFloat() {
		return boxedFloat;
	}

	public void setBoxedFloat(Float boxedFloat) {
		this.boxedFloat = boxedFloat;
	}

	public float getPrimitiveFloat() {
		return primitiveFloat;
	}

	public void setPrimitiveFloat(float primitiveFloat) {
		this.primitiveFloat = primitiveFloat;
	}

	public Double getBoxedDouble() {
		return boxedDouble;
	}

	public void setBoxedDouble(Double boxedDouble) {
		this.boxedDouble = boxedDouble;
	}

	public double getPrimitiveDouble() {
		return primitiveDouble;
	}

	public void setPrimitiveDouble(double primitiveDouble) {
		this.primitiveDouble = primitiveDouble;
	}

	public Boolean getBoxedBoolean() {
		return boxedBoolean;
	}

	public void setBoxedBoolean(Boolean boxedBoolean) {
		this.boxedBoolean = boxedBoolean;
	}

	public boolean isPrimitiveBoolean() {
		return primitiveBoolean;
	}

	public void setPrimitiveBoolean(boolean primitiveBoolean) {
		this.primitiveBoolean = primitiveBoolean;
	}

	public Instant getInstant() {
		return instant;
	}

	public void setInstant(Instant instant) {
		this.instant = instant;
	}

	public LocalDate getDate() {
		return date;
	}

	public void setDate(LocalDate date) {
		this.date = date;
	}

	public LocalTime getTime() {
		return time;
	}

	public void setTime(LocalTime time) {
		this.time = time;
	}

	public Date getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Date timestamp) {
		this.timestamp = timestamp;
	}

	public BigDecimal getBigDecimal() {
		return bigDecimal;
	}

	public void setBigDecimal(BigDecimal bigDecimal) {
		this.bigDecimal = bigDecimal;
	}

	public BigInteger getBigInteger() {
		return bigInteger;
	}

	public void setBigInteger(BigInteger bigInteger) {
		this.bigInteger = bigInteger;
	}

	public ByteBuffer getBlob() {
		return blob;
	}

	public void setBlob(ByteBuffer blob) {
		this.blob = blob;
	}

	public Set<String> getSetOfString() {
		return setOfString;
	}

	public void setSetOfString(Set<String> setOfString) {
		this.setOfString = setOfString;
	}

	public List<String> getListOfString() {
		return listOfString;
	}

	public void setListOfString(List<String> listOfString) {
		this.listOfString = listOfString;
	}

	public Map<String, String> getMapOfString() {
		return mapOfString;
	}

	public void setMapOfString(Map<String, String> mapOfString) {
		this.mapOfString = mapOfString;
	}

	public Condition getAnEnum() {
		return anEnum;
	}

	public void setAnEnum(Condition anEnum) {
		this.anEnum = anEnum;
	}

	public Set<Condition> getSetOfEnum() {
		return setOfEnum;
	}

	public void setSetOfEnum(Set<Condition> setOfEnum) {
		this.setOfEnum = setOfEnum;
	}

	public List<Condition> getListOfEnum() {
		return listOfEnum;
	}

	public void setListOfEnum(List<Condition> listOfEnum) {
		this.listOfEnum = listOfEnum;
	}

	public TupleValue getTupleValue() {
		return tupleValue;
	}

	public void setTupleValue(TupleValue tupleValue) {
		this.tupleValue = tupleValue;
	}

	public LocalDateTime getLocalDateTime() {
		return localDateTime;
	}

	public void setLocalDateTime(LocalDateTime localDateTime) {
		this.localDateTime = localDateTime;
	}

	public ZoneId getZoneId() {
		return zoneId;
	}

	public void setZoneId(ZoneId zoneId) {
		this.zoneId = zoneId;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;

		AllPossibleTypes that = (AllPossibleTypes) o;

		if (primitiveByte != that.primitiveByte)
			return false;
		if (primitiveShort != that.primitiveShort)
			return false;
		if (primitiveLong != that.primitiveLong)
			return false;
		if (primitiveInteger != that.primitiveInteger)
			return false;
		if (primitiveFloat != that.primitiveFloat)
			return false;
		if (primitiveDouble != that.primitiveDouble)
			return false;
		if (primitiveBoolean != that.primitiveBoolean)
			return false;
		if (!ObjectUtils.nullSafeEquals(id, that.id)) {
			return false;
		}
		if (!ObjectUtils.nullSafeEquals(inet, that.inet)) {
			return false;
		}
		if (!ObjectUtils.nullSafeEquals(uuid, that.uuid)) {
			return false;
		}
		if (!ObjectUtils.nullSafeEquals(justNumber, that.justNumber)) {
			return false;
		}
		if (!ObjectUtils.nullSafeEquals(boxedByte, that.boxedByte)) {
			return false;
		}
		if (!ObjectUtils.nullSafeEquals(boxedShort, that.boxedShort)) {
			return false;
		}
		if (!ObjectUtils.nullSafeEquals(boxedLong, that.boxedLong)) {
			return false;
		}
		if (!ObjectUtils.nullSafeEquals(boxedInteger, that.boxedInteger)) {
			return false;
		}
		if (!ObjectUtils.nullSafeEquals(boxedFloat, that.boxedFloat)) {
			return false;
		}
		if (!ObjectUtils.nullSafeEquals(boxedDouble, that.boxedDouble)) {
			return false;
		}
		if (!ObjectUtils.nullSafeEquals(boxedBoolean, that.boxedBoolean)) {
			return false;
		}
		if (!ObjectUtils.nullSafeEquals(instant, that.instant)) {
			return false;
		}
		if (!ObjectUtils.nullSafeEquals(date, that.date)) {
			return false;
		}
		if (!ObjectUtils.nullSafeEquals(time, that.time)) {
			return false;
		}
		if (!ObjectUtils.nullSafeEquals(timestamp, that.timestamp)) {
			return false;
		}
		if (!ObjectUtils.nullSafeEquals(bigDecimal, that.bigDecimal)) {
			return false;
		}
		if (!ObjectUtils.nullSafeEquals(bigInteger, that.bigInteger)) {
			return false;
		}
		if (!ObjectUtils.nullSafeEquals(blob, that.blob)) {
			return false;
		}
		if (!ObjectUtils.nullSafeEquals(setOfString, that.setOfString)) {
			return false;
		}
		if (!ObjectUtils.nullSafeEquals(listOfString, that.listOfString)) {
			return false;
		}
		if (!ObjectUtils.nullSafeEquals(mapOfString, that.mapOfString)) {
			return false;
		}
		if (anEnum != that.anEnum)
			return false;
		if (!ObjectUtils.nullSafeEquals(setOfEnum, that.setOfEnum)) {
			return false;
		}
		if (!ObjectUtils.nullSafeEquals(listOfEnum, that.listOfEnum)) {
			return false;
		}
		if (!ObjectUtils.nullSafeEquals(tupleValue, that.tupleValue)) {
			return false;
		}
		if (!ObjectUtils.nullSafeEquals(localDateTime, that.localDateTime)) {
			return false;
		}
		return ObjectUtils.nullSafeEquals(zoneId, that.zoneId);
	}

	@Override
	public int hashCode() {
		int result;
		long temp;
		result = ObjectUtils.nullSafeHashCode(id);
		result = 31 * result + ObjectUtils.nullSafeHashCode(inet);
		result = 31 * result + ObjectUtils.nullSafeHashCode(uuid);
		result = 31 * result + ObjectUtils.nullSafeHashCode(justNumber);
		result = 31 * result + ObjectUtils.nullSafeHashCode(boxedByte);
		result = 31 * result + (int) primitiveByte;
		result = 31 * result + ObjectUtils.nullSafeHashCode(boxedShort);
		result = 31 * result + (int) primitiveShort;
		result = 31 * result + ObjectUtils.nullSafeHashCode(boxedLong);
		result = 31 * result + (int) (primitiveLong ^ (primitiveLong >>> 32));
		result = 31 * result + ObjectUtils.nullSafeHashCode(boxedInteger);
		result = 31 * result + primitiveInteger;
		result = 31 * result + ObjectUtils.nullSafeHashCode(boxedFloat);
		result = 31 * result + (primitiveFloat != +0.0f ? Float.floatToIntBits(primitiveFloat) : 0);
		result = 31 * result + ObjectUtils.nullSafeHashCode(boxedDouble);
		temp = Double.doubleToLongBits(primitiveDouble);
		result = 31 * result + (int) (temp ^ (temp >>> 32));
		result = 31 * result + ObjectUtils.nullSafeHashCode(boxedBoolean);
		result = 31 * result + (primitiveBoolean ? 1 : 0);
		result = 31 * result + ObjectUtils.nullSafeHashCode(instant);
		result = 31 * result + ObjectUtils.nullSafeHashCode(date);
		result = 31 * result + ObjectUtils.nullSafeHashCode(time);
		result = 31 * result + ObjectUtils.nullSafeHashCode(timestamp);
		result = 31 * result + ObjectUtils.nullSafeHashCode(bigDecimal);
		result = 31 * result + ObjectUtils.nullSafeHashCode(bigInteger);
		result = 31 * result + ObjectUtils.nullSafeHashCode(blob);
		result = 31 * result + ObjectUtils.nullSafeHashCode(setOfString);
		result = 31 * result + ObjectUtils.nullSafeHashCode(listOfString);
		result = 31 * result + ObjectUtils.nullSafeHashCode(mapOfString);
		result = 31 * result + ObjectUtils.nullSafeHashCode(anEnum);
		result = 31 * result + ObjectUtils.nullSafeHashCode(setOfEnum);
		result = 31 * result + ObjectUtils.nullSafeHashCode(listOfEnum);
		result = 31 * result + ObjectUtils.nullSafeHashCode(tupleValue);
		result = 31 * result + ObjectUtils.nullSafeHashCode(localDateTime);
		result = 31 * result + ObjectUtils.nullSafeHashCode(zoneId);
		return result;
	}
}
