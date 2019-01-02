/*
 * Copyright 2013-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.cassandra.core.keyspace;

import java.util.Map;

/**
 * Enumeration that represents all known table options. If a table option is not listed here, but is supported by
 * Cassandra, use the method {@link CreateTableSpecification#with(String, Object, boolean, boolean)} to write the raw
 * value. Implements {@link Option} via delegation, since {@link Enum}s can't extend anything.
 *
 * @author Matthew T. Adams
 * @see CompactionOption
 * @see CompressionOption
 * @see CachingOption
 */
public enum TableOption implements Option {
	/**
	 * <code>comment</code>
	 */
	COMMENT("comment", String.class, true, true, true),
	/**
	 * <code>COMPACT STORAGE</code>
	 */
	COMPACT_STORAGE("COMPACT STORAGE", null, false, false, false),
	/**
	 * <code>compaction</code>. Value is a <code>Map&lt;CompactionOption,Object&gt;</code>.
	 *
	 * @see CompactionOption
	 */
	COMPACTION("compaction", Map.class, true, false, false),
	/**
	 * <code>compression</code>. Value is a <code>Map&lt;CompressionOption,Object&gt;</code>.
	 *
	 * @see {@link CompressionOption}
	 */
	COMPRESSION("compression", Map.class, true, false, false),
	/**
	 * <code>caching</code>
	 *
	 * @see CachingOption
	 */
	CACHING("caching", Map.class, true, false, false),
	/**
	 * <code>bloom_filter_fp_chance</code>
	 */
	BLOOM_FILTER_FP_CHANCE("bloom_filter_fp_chance", Double.class, true, false, false),
	/**
	 * <code>read_repair_chance</code>
	 */
	READ_REPAIR_CHANCE("read_repair_chance", Double.class, true, false, false),
	/**
	 * <code>dclocal_read_repair_chance</code>
	 */
	DCLOCAL_READ_REPAIR_CHANCE("dclocal_read_repair_chance", Double.class, true, false, false),
	/**
	 * <code>gc_grace_seconds</code>
	 */
	GC_GRACE_SECONDS("gc_grace_seconds", Long.class, true, false, false);

	private Option delegate;

	private TableOption(String name, Class<?> type, boolean requiresValue, boolean escapesValue, boolean quotesValue) {
		this.delegate = new DefaultOption(name, type, requiresValue, escapesValue, quotesValue);
	}

	@Override
	public Class<?> getType() {
		return delegate.getType();
	}

	@Override
	public boolean takesValue() {
		return delegate.takesValue();
	}

	@Override
	public String getName() {
		return delegate.getName();
	}

	@Override
	public boolean escapesValue() {
		return delegate.escapesValue();
	}

	@Override
	public boolean quotesValue() {
		return delegate.quotesValue();
	}

	@Override
	public boolean requiresValue() {
		return delegate.requiresValue();
	}

	@Override
	public void checkValue(Object value) {
		delegate.checkValue(value);
	}

	@Override
	public boolean isCoerceable(Object value) {
		return delegate.isCoerceable(value);
	}

	@Override
	public String toString() {
		return delegate.toString();
	}

	@Override
	public String toString(Object value) {
		return delegate.toString(value);
	}

	/**
	 * Known KeyCaching Options
	 *
	 * @author David Webb
	 * @since 1.2.0
	 *
	 */
	public enum KeyCachingOption {

		ALL("all"), NONE("none");

		private String value;

		private KeyCachingOption(String value) {
			this.value = value;
		}

		public String getValue() {
			return value;
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

		private CachingOption(String name, Class<?> type, boolean requiresValue, boolean escapesValue, boolean quotesValue) {
			this.delegate = new DefaultOption(name, type, requiresValue, escapesValue, quotesValue);
		}

		@Override
		public Class<?> getType() {
			return delegate.getType();
		}

		@Override
		public boolean takesValue() {
			return delegate.takesValue();
		}

		@Override
		public String getName() {
			return delegate.getName();
		}

		@Override
		public boolean escapesValue() {
			return delegate.escapesValue();
		}

		@Override
		public boolean quotesValue() {
			return delegate.quotesValue();
		}

		@Override
		public boolean requiresValue() {
			return delegate.requiresValue();
		}

		@Override
		public void checkValue(Object value) {
			delegate.checkValue(value);
		}

		@Override
		public boolean isCoerceable(Object value) {
			return delegate.isCoerceable(value);
		}

		@Override
		public String toString() {
			return delegate.toString();
		}

		@Override
		public String toString(Object value) {
			return delegate.toString(value);
		}

	}

	/**
	 * Known compaction options.
	 *
	 * @author Matthew T. Adams
	 */
	public enum CompactionOption implements Option {
		/**
		 * <code>class</code>
		 */
		CLASS("class", String.class, true, false, true),
		/**
		 * <code>tombstone_threshold</code>
		 */
		TOMBSTONE_THRESHOLD("tombstone_threshold", Double.class, true, false, false),
		/**
		 * <code>tombstone_compaction_interval</code>
		 */
		TOMBSTONE_COMPACTION_INTERVAL("tombstone_compaction_interval", Double.class, true, false, false),
		/**
		 * <code>min_sstable_size</code>
		 */
		MIN_SSTABLE_SIZE("min_sstable_size", Long.class, true, false, false),
		/**
		 * <code>min_threshold</code>
		 */
		MIN_THRESHOLD("min_threshold", Long.class, true, false, false),
		/**
		 * <code>max_threshold</code>
		 */
		MAX_THRESHOLD("max_threshold", Long.class, true, false, false),
		/**
		 * <code>bucket_low</code>
		 */
		BUCKET_LOW("bucket_low", Double.class, true, false, false),
		/**
		 * <code>bucket_high</code>
		 */
		BUCKET_HIGH("bucket_high", Double.class, true, false, false),
		/**
		 * <code>sstable_size_in_mb</code>
		 */
		SSTABLE_SIZE_IN_MB("sstable_size_in_mb", Long.class, true, false, false);

		private Option delegate;

		private CompactionOption(String name, Class<?> type, boolean requiresValue, boolean escapesValue,
				boolean quotesValue) {
			this.delegate = new DefaultOption(name, type, requiresValue, escapesValue, quotesValue);
		}

		@Override
		public Class<?> getType() {
			return delegate.getType();
		}

		@Override
		public boolean takesValue() {
			return delegate.takesValue();
		}

		@Override
		public String getName() {
			return delegate.getName();
		}

		@Override
		public boolean escapesValue() {
			return delegate.escapesValue();
		}

		@Override
		public boolean quotesValue() {
			return delegate.quotesValue();
		}

		@Override
		public boolean requiresValue() {
			return delegate.requiresValue();
		}

		@Override
		public void checkValue(Object value) {
			delegate.checkValue(value);
		}

		@Override
		public boolean isCoerceable(Object value) {
			return delegate.isCoerceable(value);
		}

		@Override
		public String toString() {
			return delegate.toString();
		}

		@Override
		public String toString(Object value) {
			return delegate.toString(value);
		}
	}

	/**
	 * Known compression options.
	 *
	 * @author Matthew T. Adams
	 */
	public enum CompressionOption implements Option {
		/**
		 * <code>sstable_compression</code>
		 */
		SSTABLE_COMPRESSION("sstable_compression", String.class, true, false, true),
		/**
		 * <code>chunk_length_kb</code>
		 */
		CHUNK_LENGTH_KB("chunk_length_kb", Long.class, true, false, false),
		/**
		 * <code>crc_check_chance</code>
		 */
		CRC_CHECK_CHANCE("crc_check_chance", Double.class, true, false, false);

		private Option delegate;

		private CompressionOption(String name, Class<?> type, boolean requiresValue, boolean escapesValue,
				boolean quotesValue) {
			this.delegate = new DefaultOption(name, type, requiresValue, escapesValue, quotesValue);
		}

		@Override
		public Class<?> getType() {
			return delegate.getType();
		}

		@Override
		public boolean takesValue() {
			return delegate.takesValue();
		}

		@Override
		public String getName() {
			return delegate.getName();
		}

		@Override
		public boolean escapesValue() {
			return delegate.escapesValue();
		}

		@Override
		public boolean quotesValue() {
			return delegate.quotesValue();
		}

		@Override
		public boolean requiresValue() {
			return delegate.requiresValue();
		}

		@Override
		public void checkValue(Object value) {
			delegate.checkValue(value);
		}

		@Override
		public boolean isCoerceable(Object value) {
			return delegate.isCoerceable(value);
		}

		@Override
		public String toString() {
			return delegate.toString();
		}

		@Override
		public String toString(Object value) {
			return delegate.toString(value);
		}
	}
}
