package org.hibernate.rx.impl;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.hibernate.sql.exec.spi.ExecutionContext;

import io.reactiverse.pgclient.PgPreparedQuery;

public interface RxValueBinder<J> {

	/**
	 * Bind a value to a prepared statement.
	 */
	void bind(PgPreparedQuery preparedQuery, int parameterPosition, J value, ExecutionContext executionContext) throws SQLException;
}
