package org.springframework.data.cassandra.mapping;

import java.lang.reflect.Field;
import java.util.Comparator;

/**
 * {@link Comparator} implementation that orders {@link CassandraPersistentProperty} instances.
 * <p/>
 * Composite primary key properties and primary key properties sort before non-primary key properties.
 * 
 * @author Alex Shvid
 * @author Matthew T. Adams
 */
public enum CassandraPersistentPropertyComparator implements Comparator<CassandraPersistentProperty> {

	/**
	 * The sole instance of this class.
	 */
	IT;

	@Override
	public int compare(CassandraPersistentProperty left, CassandraPersistentProperty right) {

		if (left != null && right == null) {
			return -1;
		}
		if (left == null && right != null) {
			return 1;
		}
		if (left == null && right == null) {
			return 0;
		}

		if (left.equals(right)) {
			return 0;
		}

		boolean leftIsCompositePrimaryKey = left.isCompositePrimaryKey();
		boolean rightIsCompositePrimaryKey = right.isCompositePrimaryKey();

		if (leftIsCompositePrimaryKey && rightIsCompositePrimaryKey) {
			return 0;
		}

		boolean leftIsPrimaryKey = left.isPrimaryKeyColumn();
		boolean rightIsPrimaryKey = right.isPrimaryKeyColumn();

		Field leftField = left.getField();
		Field rightField = right.getField();

		if (leftIsPrimaryKey && rightIsPrimaryKey) {
			return CassandraPrimaryKeyColumnAnnotationComparator.IT.compare(leftField.getAnnotation(PrimaryKeyColumn.class),
					rightField.getAnnotation(PrimaryKeyColumn.class));
		}

		boolean leftIsKey = leftIsCompositePrimaryKey || leftIsPrimaryKey;
		boolean rightIsKey = rightIsCompositePrimaryKey || rightIsPrimaryKey;

		if (leftIsKey && !rightIsKey) {
			return 1;
		}

		if (!leftIsKey && rightIsKey) {
			return -1;
		}

		// else, neither property is a composite primary key nor a primary key; compare @Column annotations

		Column leftColumn = leftField.getAnnotation(Column.class);
		Column rightColumn = rightField.getAnnotation(Column.class);

		if (leftColumn == null && rightColumn == null) {
			return leftField.getName().compareTo(rightField.getName());
		}

		if (leftColumn != null && rightColumn != null) {
			return CassandraColumnAnnotationComparator.IT.compare(leftColumn, rightColumn);
		}

		if (leftColumn != null && rightColumn == null) {
			return leftColumn.value().compareTo(rightField.getName());
		}

		// else leftColumn == null && rightColumn != null)
		return leftField.getName().compareTo(rightColumn.value());
	}
}