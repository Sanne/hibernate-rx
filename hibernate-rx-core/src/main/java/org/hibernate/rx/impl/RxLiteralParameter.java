package org.hibernate.rx.impl;

import org.hibernate.rx.sql.exec.spi.RxParameterBinder;
import org.hibernate.sql.SqlExpressableType;
import org.hibernate.sql.ast.consume.spi.SqlAstWalker;
import org.hibernate.sql.ast.tree.spi.expression.GenericParameter;
import org.hibernate.sql.ast.tree.spi.expression.LiteralParameter;
import org.hibernate.sql.exec.spi.ExecutionContext;
import org.hibernate.sql.exec.spi.JdbcParameterBinder;
import org.hibernate.sql.results.spi.SqlSelection;
import org.hibernate.type.descriptor.java.spi.BasicJavaDescriptor;
import org.hibernate.type.spi.TypeConfiguration;

import io.reactiverse.pgclient.PgPreparedQuery;

public class RxLiteralParameter implements GenericParameter, RxParameterBinder {

	private final LiteralParameter delegate;

	public RxLiteralParameter(LiteralParameter delegate) {
		this.delegate = delegate;
	}

	@Override
	public JdbcParameterBinder getParameterBinder() {
		return delegate.getParameterBinder();
	}

	@Override
	public SqlExpressableType getExpressableType() {
		return delegate.getExpressableType();
	}

	@Override
	public SqlSelection createSqlSelection(
			int jdbcPosition,
			int valuesArrayPosition,
			BasicJavaDescriptor javaTypeDescriptor,
			TypeConfiguration typeConfiguration) {
		return delegate.createSqlSelection( jdbcPosition, valuesArrayPosition, javaTypeDescriptor, typeConfiguration );
	}

	@Override
	public void accept(SqlAstWalker sqlTreeWalker) {
		delegate.accept( sqlTreeWalker );
	}

	@Override
	public int bindParameterValue(
			PgPreparedQuery statement, int startPosition, ExecutionContext executionContext) {
		delegate.getExpressableType();
		return 1;
	}
}
