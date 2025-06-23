/*
 * Copyright 2013-2025 the original author or authors.
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
package org.springframework.data.cassandra.core.cql.keyspace;

import java.util.Map;

import org.jspecify.annotations.Nullable;

/**
 * Enumeration that represents all known table options. If a table option is not listed here, but is supported by
 * Cassandra, use the method {@link CreateTableSpecification#with(String, Object, boolean, boolean)} to write the raw
 * value. Implements {@link Option} via delegation, since {@link Enum}s can't extend anything.
 *
 * @author Matthew T. Adams
 * @author Mark Paluch
 * @author Mikhail Polivakha
 * @author Seungho Kang
 * @see CompactionOption
 * @see CompressionOption
 * @see CachingOption
 */
public enum TableOption implements Option {

	/**
	 * {@code comment}
	 */
	COMMENT("comment", String.class, true, true, true),
	/**
	 * {@code COMPACT STORAGE}
	 *
	 * @deprecated since 2.2. Cassandra 4.x has deprecated compact storage.
	 */
	@Deprecated
	COMPACT_STORAGE("COMPACT STORAGE", Void.class, false, false, false),
	/**
	 * {@code compaction}. Value is a <code>Map&lt;CompactionOption,Object&gt;</code>.
	 *
	 * @see CompactionOption
	 */
	COMPACTION("compaction", Map.class, true, false, false),
	/**
	 * {@code compression}. Value is a <code>Map&lt;CompressionOption,Object&gt;</code>.
	 *
	 * @see CompressionOption
	 */
	COMPRESSION("compression", Map.class, true, false, false),
	/**
	 * {@code caching}
	 *
	 * @see CachingOption
	 */
	CACHING("caching", Map.class, true, false, false),
	/**
	 * {@code bloom_filter_fp_chance}
	 */
	BLOOM_FILTER_FP_CHANCE("bloom_filter_fp_chance", Double.class, true, false, false),
	/**
	 * {@code read_repair_chance}
	 */
	READ_REPAIR_CHANCE("read_repair_chance", Double.class, true, false, false),
	/**
	 * {@code dclocal_read_repair_chance}
	 */
	DCLOCAL_READ_REPAIR_CHANCE("dclocal_read_repair_chance", Double.class, true, false, false),
	/**
	 * {@code gc_grace_seconds}
	 */
	GC_GRACE_SECONDS("gc_grace_seconds", Long.class, true, false, false),
	/**
	 * {@code default_time_to_live}
	 */
	DEFAULT_TIME_TO_LIVE("default_time_to_live", Long.class, true, false, false),
	/**
	 * {@code cdc}
	 */
	CDC("cdc", Boolean.class, true, false, false),
	/**
	 * {@code speculative_retry}
	 */
	SPECULATIVE_RETRY("speculative_retry", String.class, true, true, true),
	/**
	 * {@code memtable_flush_period_in_ms}
	 */
	MEMTABLE_FLUSH_PERIOD_IN_MS("memtable_flush_period_in_ms", Long.class, true, false, false),
	/**
	 * {@code crc_check_chance}
	 */
	CRC_CHECK_CHANCE("crc_check_chance", Double.class, true, false, false),
	/**
	 * {@code min_index_interval}
	 */
	MIN_INDEX_INTERVAL("min_index_interval", Long.class, true, false, false),
	/**
	 * {@code max_index_interval}
	 */
	MAX_INDEX_INTERVAL("max_index_interval", Long.class, true, false, false),
	/**
	 * {@code read_repair}
	 */
	READ_REPAIR("read_repair", String.class, true, true, true);

	private Option delegate;

	TableOption(String name, Class<?> type, boolean requiresValue, boolean escapesValue, boolean quotesValue) {
		this.delegate = new DefaultOption(name, type, requiresValue, escapesValue, quotesValue);
	}

	/**
	 * Look up {@link TableOption} by name using case-insensitive lookups.
	 *
	 * @param optionName name of the option.
	 * @return the option.
	 * @throws IllegalArgumentException if the option cannot be determined.
	 * @since 4.1.1
	 */
	public static TableOption valueOfIgnoreCase(String optionName) {
		for (TableOption value : values()) {
			if (value.getName().equalsIgnoreCase(optionName)) {
				return value;
			}
		}
		throw new IllegalArgumentException(String.format("Unable to recognize specified Table option '%s'", optionName));
	}

	/**
	 * Look up {@link TableOption} by name using case-insensitive lookups.
	 *
	 * @param optionName name of the option.
	 * @return the matching {@link TableOption}, or {@code null} if no match is found
	 * @since 4.5.2
	 */
	@Nullable
	public static TableOption findByNameIgnoreCase(String optionName) {
		for (TableOption value : values()) {
			if (value.getName().equalsIgnoreCase(optionName)) {
				return value;
			}
		}
		return null;
	}

	@Override
	public Class<?> getType() {
		return this.delegate.getType();
	}

	@Override
	public boolean takesValue() {
		return this.delegate.takesValue();
	}

	@Override
	public String getName() {
		return this.delegate.getName();
	}

	@Override
	public boolean escapesValue() {
		return this.delegate.escapesValue();
	}

	@Override
	public boolean quotesValue() {
		return this.delegate.quotesValue();
	}

	@Override
	public boolean requiresValue() {
		return this.delegate.requiresValue();
	}

	@Override
	public void checkValue(Object value) {
		this.delegate.checkValue(value);
	}

	@Override
	public boolean isCoerceable(Object value) {
		return this.delegate.isCoerceable(value);
	}

	@Override
	public String toString() {
		return this.delegate.toString();
	}

	@Override
	public String toString(@Nullable Object value) {
		return this.delegate.toString(value);
	}

	/**
	 * Known KeyCaching Options
	 *
	 * @author David Webb
	 * @since 1.2.0
	 */
	public enum KeyCachingOption {

		ALL("all"), NONE("none");

		private String value;

		KeyCachingOption(String value) {
			this.value = value;
		}

		public String getValue() {
			return this.value;
		}

		@Override
		public String toString() {
			return getValue();
		}
	}

	/**
	 * Known caching options.
	 *
	 * @author Matthew T. Adams
	 * @author David Webb
	 * @since 1.2.0
	 */
	public enum CachingOption implements Option {

		KEYS("keys", KeyCachingOption.class, true, false, true),

		ROWS_PER_PARTITION("rows_per_partition", String.class, true, false, true);

		private Option delegate;

		CachingOption(String name, Class<?> type, boolean requiresValue, boolean escapesValue, boolean quotesValue) {
			this.delegate = new DefaultOption(name, type, requiresValue, escapesValue, quotesValue);
		}

		@Override
		public Class<?> getType() {
			return this.delegate.getType();
		}

		@Override
		public boolean takesValue() {
			return this.delegate.takesValue();
		}

		@Override
		public String getName() {
			return this.delegate.getName();
		}

		@Override
		public boolean escapesValue() {
			return this.delegate.escapesValue();
		}

		@Override
		public boolean quotesValue() {
			return this.delegate.quotesValue();
		}

		@Override
		public boolean requiresValue() {
			return this.delegate.requiresValue();
		}

		@Override
		public void checkValue(Object value) {
			this.delegate.checkValue(value);
		}

		@Override
		public boolean isCoerceable(Object value) {
			return this.delegate.isCoerceable(value);
		}

		@Override
		public String toString() {
			return this.delegate.toString();
		}

		@Override
		public String toString(@Nullable Object value) {
			return this.delegate.toString(value);
		}

	}

	/**
	 * Known compaction options.
	 *
	 * @author Matthew T. Adams
	 */
	public enum CompactionOption implements Option {

		/**
		 * {@code class}
		 */
		CLASS("class", String.class, true, false, true),
		/**
		 * {@code tombstone_threshold}
		 */
		TOMBSTONE_THRESHOLD("tombstone_threshold", Double.class, true, false, false),
		/**
		 * {@code tombstone_compaction_interval}
		 */
		TOMBSTONE_COMPACTION_INTERVAL("tombstone_compaction_interval", Double.class, true, false, false),
		/**
		 * {@code min_sstable_size}
		 */
		MIN_SSTABLE_SIZE("min_sstable_size", Long.class, true, false, false),
		/**
		 * {@code min_threshold}
		 */
		MIN_THRESHOLD("min_threshold", Long.class, true, false, false),
		/**
		 * {@code max_threshold}
		 */
		MAX_THRESHOLD("max_threshold", Long.class, true, false, false),
		/**
		 * {@code bucket_low}
		 */
		BUCKET_LOW("bucket_low", Double.class, true, false, false),
		/**
		 * {@code bucket_high}
		 */
		BUCKET_HIGH("bucket_high", Double.class, true, false, false),
		/**
		 * {@code sstable_size_in_mb}
		 */
		SSTABLE_SIZE_IN_MB("sstable_size_in_mb", Long.class, true, false, false);

		private Option delegate;

		CompactionOption(String name, Class<?> type, boolean requiresValue, boolean escapesValue, boolean quotesValue) {
			this.delegate = new DefaultOption(name, type, requiresValue, escapesValue, quotesValue);
		}

		@Override
		public Class<?> getType() {
			return this.delegate.getType();
		}

		@Override
		public boolean takesValue() {
			return this.delegate.takesValue();
		}

		@Override
		public String getName() {
			return this.delegate.getName();
		}

		@Override
		public boolean escapesValue() {
			return this.delegate.escapesValue();
		}

		@Override
		public boolean quotesValue() {
			return this.delegate.quotesValue();
		}

		@Override
		public boolean requiresValue() {
			return this.delegate.requiresValue();
		}

		@Override
		public void checkValue(Object value) {
			this.delegate.checkValue(value);
		}

		@Override
		public boolean isCoerceable(Object value) {
			return this.delegate.isCoerceable(value);
		}

		@Override
		public String toString() {
			return this.delegate.toString();
		}

		@Override
		public String toString(@Nullable Object value) {
			return this.delegate.toString(value);
		}
	}

	/**
	 * Known compression options.
	 *
	 * @author Matthew T. Adams
	 */
	public enum CompressionOption implements Option {

		/**
		 * {@code sstable_compression}
		 */
		SSTABLE_COMPRESSION("sstable_compression", String.class, true, false, true),
		/**
		 * {@code chunk_length_kb}
		 */
		CHUNK_LENGTH_KB("chunk_length_kb", Long.class, true, false, false),
		/**
		 * {@code crc_check_chance}
		 */
		CRC_CHECK_CHANCE("crc_check_chance", Double.class, true, false, false);

		private Option delegate;

		CompressionOption(String name, Class<?> type, boolean requiresValue, boolean escapesValue, boolean quotesValue) {
			this.delegate = new DefaultOption(name, type, requiresValue, escapesValue, quotesValue);
		}

		@Override
		public Class<?> getType() {
			return this.delegate.getType();
		}

		@Override
		public boolean takesValue() {
			return this.delegate.takesValue();
		}

		@Override
		public String getName() {
			return this.delegate.getName();
		}

		@Override
		public boolean escapesValue() {
			return this.delegate.escapesValue();
		}

		@Override
		public boolean quotesValue() {
			return this.delegate.quotesValue();
		}

		@Override
		public boolean requiresValue() {
			return this.delegate.requiresValue();
		}

		@Override
		public void checkValue(Object value) {
			this.delegate.checkValue(value);
		}

		@Override
		public boolean isCoerceable(Object value) {
			return this.delegate.isCoerceable(value);
		}

		@Override
		public String toString() {
			return this.delegate.toString();
		}

		@Override
		public String toString(@Nullable Object value) {
			return this.delegate.toString(value);
		}
	}
}
