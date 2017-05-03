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

import org.springframework.data.cassandra.convert.CassandraTypeMappingIntegrationTest.Condition;
import org.springframework.data.cassandra.mapping.CassandraType;
import org.springframework.data.cassandra.mapping.PrimaryKey;
import org.springframework.data.cassandra.mapping.Table;

import com.datastax.driver.core.DataType.Name;

/**
 * @author Mark Paluch
 */
@Table
@Data
@NoArgsConstructor
@RequiredArgsConstructor
public class AllPossibleTypes {

	@PrimaryKey @NonNull private String id;

	private InetAddress inet;

	@CassandraType(type = Name.UUID) private UUID uuid;

	@CassandraType(type = Name.INT) private Number justNumber;

	private Byte boxedByte;
	private byte primitiveByte;

	private Short boxedShort;
	private short primitiveShort;

	private Long boxedLong;
	private long primitiveLong;

	private Integer boxedInteger;
	private int primitiveInteger;

	private Float boxedFloat;
	private float primitiveFloat;

	private Double boxedDouble;
	private double primitiveDouble;

	private Boolean boxedBoolean;
	private boolean primitiveBoolean;

	private com.datastax.driver.core.LocalDate date;

	private Date timestamp;

	private BigDecimal bigDecimal;
	private BigInteger bigInteger;
	private ByteBuffer blob;

	private Set<String> setOfString;
	private List<String> listOfString;
	private Map<String, String> mapOfString;

	private Condition anEnum;
	private Set<Condition> setOfEnum;
	private List<Condition> listOfEnum;

	// supported by conversion
	java.time.LocalDate localDate;
	java.time.LocalDateTime localDateTime;
	java.time.LocalTime localTime;
	java.time.Instant instant;
	java.time.ZoneId zoneId;

	org.joda.time.LocalDate jodaLocalDate;
	org.joda.time.LocalDateTime jodaLocalDateTime;
	org.joda.time.DateTime jodaDateTime;

	org.threeten.bp.LocalDate bpLocalDate;
	org.threeten.bp.LocalDateTime bpLocalDateTime;
	org.threeten.bp.LocalTime bpLocalTime;
	org.threeten.bp.Instant bpInstant;
	org.threeten.bp.ZoneId bpZoneId;

}
