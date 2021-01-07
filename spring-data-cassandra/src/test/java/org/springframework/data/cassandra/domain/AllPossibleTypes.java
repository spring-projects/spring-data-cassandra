/*
 * Copyright 2016-2021 the original author or authors.
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

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.springframework.data.cassandra.core.convert.CassandraTypeMappingIntegrationTests.Condition;
import org.springframework.data.cassandra.core.mapping.CassandraType;
import org.springframework.data.cassandra.core.mapping.PrimaryKey;
import org.springframework.data.cassandra.core.mapping.Table;

import com.datastax.oss.driver.api.core.data.TupleValue;

/**
 * @author Mark Paluch
 */
@Table
@Data
@NoArgsConstructor
@RequiredArgsConstructor
public class AllPossibleTypes {

	@PrimaryKey @NonNull String id;

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

	org.joda.time.DateTime jodaDateTime;
	org.joda.time.LocalDate jodaLocalDate;
	org.joda.time.LocalDateTime jodaLocalDateTime;
	org.joda.time.LocalTime jodaLocalTime;

	org.threeten.bp.Instant bpInstant;
	org.threeten.bp.LocalDate bpLocalDate;
	org.threeten.bp.LocalDateTime bpLocalDateTime;
	org.threeten.bp.LocalTime bpLocalTime;
	org.threeten.bp.ZoneId bpZoneId;

}
