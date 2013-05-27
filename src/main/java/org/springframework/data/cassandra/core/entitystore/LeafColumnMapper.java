package org.springframework.data.cassandra.core.entitystore;

import java.lang.reflect.Field;
import java.util.Iterator;

import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.Serializer;

class LeafColumnMapper extends AbstractColumnMapper {
	
	private final Serializer<?> serializer;

	LeafColumnMapper(final Field field) {
	    super(field);
		this.serializer = MappingUtils.getSerializerForField(field);
	}

	public String getColumnName() {
		return columnName;
	}
	
	Serializer<?> getSerializer() {
	    return serializer;
	}
	
	@SuppressWarnings("unchecked")
	public boolean fillMutationBatch(Object entity, ColumnListMutation<String> clm, String prefix) throws Exception {
		Object value = field.get(entity);
		if(value == null) {
			if(columnAnnotation.nullable())
				return false; // skip
			else
				throw new IllegalArgumentException("cannot write non-nullable column with null value: " + columnName);
		}
		@SuppressWarnings("rawtypes")
		final Serializer valueSerializer = serializer;
		// TODO: suppress the unchecked raw type now.
		// we have to use the raw type to avoid compiling error
		clm.putColumn(prefix + columnName, value, valueSerializer, null);
		return true;
	}
	
    public boolean setField(Object entity, Iterator<String> name, com.netflix.astyanax.model.Column<String> column) throws Exception {
        if (name.hasNext()) 
            return false;
        final Object fieldValue = column.getValue(serializer);
        field.set(entity, fieldValue);
        return true;
    }

    public void validate(Object entity) throws Exception {
        if (field.get(entity) == null && !columnAnnotation.nullable())
            throw new IllegalArgumentException("cannot find non-nullable column: " + columnName);
    }
}
