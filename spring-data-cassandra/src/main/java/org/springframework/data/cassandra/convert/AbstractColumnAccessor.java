package org.springframework.data.cassandra.convert;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.springframework.cassandra.core.cql.CqlIdentifier;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.GettableData;
import com.datastax.driver.core.SettableData;
import com.datastax.driver.core.UDTValue;

/**
 * @author Fabio Mendes <fabiojmendes@gmail.com> [Mar 17, 2015]
 *
 */
public abstract class AbstractColumnAccessor {
	protected GettableData dataGetter;
	private SettableData<?> dataSetter;

	/**
	 * @param dataGetter
	 */
	public AbstractColumnAccessor(GettableData dataGetter) {
		this.dataGetter = dataGetter;
	}

	/**
	 * @param dataGetter
	 * @param dataSetter
	 */
	public AbstractColumnAccessor(GettableData dataGetter, SettableData<?> dataSetter) {
		this.dataGetter = dataGetter;
		this.dataSetter = dataSetter;
	}

	abstract protected DataType getDataType(int i);
	
	abstract protected int getColumnIndex(String name);
	
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

		if (dataGetter.isNull(i)) {
			return null;
		}

		DataType type = getDataType(i);

		if (type.isCollection()) {

			List<DataType> collectionTypes = type.getTypeArguments();
			if (collectionTypes.size() == 2) {
				return dataGetter.getMap(i, collectionTypes.get(0).asJavaClass(), collectionTypes.get(1).asJavaClass());
			}

			if (type.equals(DataType.list(collectionTypes.get(0)))) {
				return dataGetter.getList(i, collectionTypes.get(0).asJavaClass());
			}

			if (type.equals(DataType.set(collectionTypes.get(0)))) {
				return dataGetter.getSet(i, collectionTypes.get(0).asJavaClass());
			}

			throw new IllegalStateException("Unknown Collection type encountered.  Valid collections are Set, List and Map.");
		}

		if (type.equals(DataType.text()) || type.equals(DataType.ascii()) || type.equals(DataType.varchar())) {
			return dataGetter.getString(i);
		}
		if (type.equals(DataType.cint())) {
			return new Integer(dataGetter.getInt(i));
		}
		if (type.equals(DataType.varint())) {
			return dataGetter.getVarint(i);
		}
		if (type.equals(DataType.cdouble())) {
			return new Double(dataGetter.getDouble(i));
		}
		if (type.equals(DataType.bigint()) || type.equals(DataType.counter())) {
			return new Long(dataGetter.getLong(i));
		}
		if (type.equals(DataType.cfloat())) {
			return new Float(dataGetter.getFloat(i));
		}
		if (type.equals(DataType.decimal())) {
			return dataGetter.getDecimal(i);
		}
		if (type.equals(DataType.cboolean())) {
			return new Boolean(dataGetter.getBool(i));
		}
		if (type.equals(DataType.timestamp())) {
			return dataGetter.getDate(i);
		}
		if (type.equals(DataType.blob())) {
			return dataGetter.getBytes(i);
		}
		if (type.equals(DataType.inet())) {
			return dataGetter.getInet(i);
		}
		if (type.equals(DataType.uuid()) || type.equals(DataType.timeuuid())) {
			return dataGetter.getUUID(i);
		}

		return dataGetter.getBytesUnsafe(i);
	}

	/**
	 * Returns the row's column value.
	 */
	public void set(CqlIdentifier name, Object value) {
		set(name.toCql(), value);
	}

	/**
	 * Returns the row's column value.
	 */
	public void set(String name, Object value) {
		int indexOf = getColumnIndex(name);
		set(indexOf, value);
	}

	public void set(int i, Object value) {
		
		if (dataSetter == null) {
			throw new IllegalStateException("This data object is read-only");
		}
		
		if (value == null) {
			dataSetter.setToNull(i);
			return;
		}

		DataType type = getDataType(i);

		if (type.isCollection()) {

			List<DataType> collectionTypes = type.getTypeArguments();
			if (collectionTypes.size() == 2) {
				dataSetter.setMap(i, (Map<?,?>) value);
			}

			if (type.equals(DataType.list(collectionTypes.get(0)))) {
				dataSetter.setList(i, (List<?>) value);
			}

			if (type.equals(DataType.set(collectionTypes.get(0)))) {
				dataSetter.setSet(i, (Set<?>) value);
			}

			throw new IllegalStateException("Unknown Collection type encountered.  Valid collections are Set, List and Map.");
		} else if (type.equals(DataType.text()) || type.equals(DataType.ascii()) || type.equals(DataType.varchar())) {
			dataSetter.setString(i, (String) value);
		} else if (type.equals(DataType.cint())) {
			dataSetter.setInt(i, (Integer) value);
		} else if (type.equals(DataType.varint())) {
			dataSetter.setVarint(i, (BigInteger) value);
		} else if (type.equals(DataType.cdouble())) {
			dataSetter.setDouble(i, (Double) value);
		} else if (type.equals(DataType.bigint()) || type.equals(DataType.counter())) {
			dataSetter.setLong(i, (Long) value);
		} else if (type.equals(DataType.cfloat())) {
			dataSetter.setFloat(i, (Float) value);
		} else if (type.equals(DataType.decimal())) {
			dataSetter.setDecimal(i, (BigDecimal) value);
		} else if (type.equals(DataType.cboolean())) {
			dataSetter.setBool(i, (Boolean) value);
		} else if (type.equals(DataType.timestamp())) {
			dataSetter.setDate(i, (Date) value);
		} else if (type.equals(DataType.blob())) {
			dataSetter.setBytes(i, (ByteBuffer) value);
		} else if (type.equals(DataType.inet())) {
			dataSetter.setInet(i, (InetAddress) value);
		} else if (type.equals(DataType.uuid()) || type.equals(DataType.timeuuid())) {
			dataSetter.setUUID(i, (UUID) value);
		} else if (type.getName().equals(DataType.Name.UDT)) {
			dataSetter.setUDTValue(i, (UDTValue) value);
		} else {
			dataSetter.setBytesUnsafe(i, (ByteBuffer) value);
		}
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
