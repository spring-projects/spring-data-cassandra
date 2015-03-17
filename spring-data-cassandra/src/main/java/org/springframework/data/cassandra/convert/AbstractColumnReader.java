package org.springframework.data.cassandra.convert;

import java.util.List;

import org.springframework.cassandra.core.cql.CqlIdentifier;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.GettableData;

public abstract class AbstractColumnReader {
	protected GettableData data;

	public AbstractColumnReader(GettableData data) {
		this.data = data;
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

	abstract protected DataType getDataType(int i);

	abstract protected int getColumnIndex(String name);

	public Object get(int i) {

		if (data.isNull(i)) {
			return null;
		}

		DataType type = getDataType(i);

		if (type.isCollection()) {

			List<DataType> collectionTypes = type.getTypeArguments();
			if (collectionTypes.size() == 2) {
				return data.getMap(i, collectionTypes.get(0).asJavaClass(), collectionTypes.get(1).asJavaClass());
			}

			if (type.equals(DataType.list(collectionTypes.get(0)))) {
				return data.getList(i, collectionTypes.get(0).asJavaClass());
			}

			if (type.equals(DataType.set(collectionTypes.get(0)))) {
				return data.getSet(i, collectionTypes.get(0).asJavaClass());
			}

			throw new IllegalStateException("Unknown Collection type encountered.  Valid collections are Set, List and Map.");
		}

		if (type.equals(DataType.text()) || type.equals(DataType.ascii()) || type.equals(DataType.varchar())) {
			return data.getString(i);
		}
		if (type.equals(DataType.cint())) {
			return new Integer(data.getInt(i));
		}
		if (type.equals(DataType.varint())) {
			return data.getVarint(i);
		}
		if (type.equals(DataType.cdouble())) {
			return new Double(data.getDouble(i));
		}
		if (type.equals(DataType.bigint()) || type.equals(DataType.counter())) {
			return new Long(data.getLong(i));
		}
		if (type.equals(DataType.cfloat())) {
			return new Float(data.getFloat(i));
		}
		if (type.equals(DataType.decimal())) {
			return data.getDecimal(i);
		}
		if (type.equals(DataType.cboolean())) {
			return new Boolean(data.getBool(i));
		}
		if (type.equals(DataType.timestamp())) {
			return data.getDate(i);
		}
		if (type.equals(DataType.blob())) {
			return data.getBytes(i);
		}
		if (type.equals(DataType.inet())) {
			return data.getInet(i);
		}
		if (type.equals(DataType.uuid()) || type.equals(DataType.timeuuid())) {
			return data.getUUID(i);
		}

		return data.getBytesUnsafe(i);
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
		return get(getColumnIndex(name), requestedType);
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
}
