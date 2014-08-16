package org.springframework.data.cassandra.convert;

import java.util.List;

import org.springframework.cassandra.core.cql.CqlIdentifier;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;

/**
 * Helpful class to read a column's value from a row, with possible type conversion.
 * 
 * @author Matthew T. Adams
 */
public class ColumnReader {

	protected Row row;
	protected ColumnDefinitions columns;

	public ColumnReader(Row row) {
		this.row = row;
		this.columns = row.getColumnDefinitions();
	}

	/**
	 * Returns the row's column value.
	 */
	public Object get(CqlIdentifier name) {
		return get(name.toCql());
	}

	/**
	 * Returns the row's column value.
	 */
	public Object get(String name) {
		int indexOf = getColumnIndex(name);
		return get(indexOf);
	}

	public Object get(int i) {

		if (row.isNull(i)) {
			return null;
		}

		DataType type = columns.getType(i);

		if (type.isCollection()) {

			List<DataType> collectionTypes = type.getTypeArguments();
			if (collectionTypes.size() == 2) {
				return row.getMap(i, collectionTypes.get(0).asJavaClass(), collectionTypes.get(1).asJavaClass());
			}

			if (type.equals(DataType.list(collectionTypes.get(0)))) {
				return row.getList(i, collectionTypes.get(0).asJavaClass());
			}

			if (type.equals(DataType.set(collectionTypes.get(0)))) {
				return row.getSet(i, collectionTypes.get(0).asJavaClass());
			}

			throw new IllegalStateException("Unknown Collection type encountered.  Valid collections are Set, List and Map.");
		}

		if (type.equals(DataType.text()) || type.equals(DataType.ascii()) || type.equals(DataType.varchar())) {
			return row.getString(i);
		}
		if (type.equals(DataType.cint()) || type.equals(DataType.varint())) {
			return new Integer(row.getInt(i));
		}
		if (type.equals(DataType.cdouble())) {
			return new Double(row.getDouble(i));
		}
		if (type.equals(DataType.bigint()) || type.equals(DataType.counter())) {
			return new Long(row.getLong(i));
		}
		if (type.equals(DataType.cfloat())) {
			return new Float(row.getFloat(i));
		}
		if (type.equals(DataType.decimal())) {
			return row.getDecimal(i);
		}
		if (type.equals(DataType.cboolean())) {
			return new Boolean(row.getBool(i));
		}
		if (type.equals(DataType.timestamp())) {
			return row.getDate(i);
		}
		if (type.equals(DataType.blob())) {
			return row.getBytes(i);
		}
		if (type.equals(DataType.inet())) {
			return row.getInet(i);
		}
		if (type.equals(DataType.uuid()) || type.equals(DataType.timeuuid())) {
			return row.getUUID(i);
		}

		return row.getBytesUnsafe(i);
	}

	public Row getRow() {
		return row;
	}

	/**
	 * Returns the row's column value as an instance of the given type.
	 * 
	 * @throws ClassCastException if the value cannot be converted to the requested type.
	 */
	public <T> T get(CqlIdentifier name, Class<T> requestedType) {
		return get(getColumnIndex(name.toCql()), requestedType);
	}

	/**
	 * Returns the row's column value as an instance of the given type.
	 * 
	 * @throws ClassCastException if the value cannot be converted to the requested type.
	 */
	public <T> T get(String name, Class<T> requestedType) {
		return get(columns.getIndexOf(name), requestedType);
	}

	/**
	 * Returns the row's column value as an instance of the given type.
	 * 
	 * @throws ClassCastException if the value cannot be converted to the requested type.
	 */
	@SuppressWarnings("unchecked")
	public <T> T get(int i, Class<T> requestedType) {

		Object o = get(i);

		if (o == null) {
			return null;
		}

		return (T) o;
	}

	private int getColumnIndex(String name) {
		int indexOf = columns.getIndexOf(name);
		if (indexOf == -1) {
			throw new IllegalArgumentException("Column does not exist in Cassandra table: " + name);
		}
		return indexOf;
	}

}
