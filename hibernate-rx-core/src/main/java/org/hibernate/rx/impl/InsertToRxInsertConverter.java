package org.hibernate.rx.impl;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.metamodel.model.relational.spi.PhysicalTable;
import org.hibernate.rx.sql.exec.spi.RxMutation;
import org.hibernate.sql.ast.consume.spi.AbstractSqlAstToJdbcOperationConverter;
import org.hibernate.sql.ast.consume.spi.InsertToJdbcInsertConverter;
import org.hibernate.sql.ast.consume.spi.SqlMutationToJdbcMutationConverter;
import org.hibernate.sql.ast.tree.spi.InsertStatement;
import org.hibernate.sql.ast.tree.spi.expression.ColumnReference;
import org.hibernate.sql.ast.tree.spi.expression.Expression;
import org.hibernate.sql.exec.spi.JdbcInsert;
import org.hibernate.sql.exec.spi.JdbcParameterBinder;

public class InsertToRxInsertConverter extends AbstractSqlAstToJdbcOperationConverter
	// TODO: Create a Rx interface instead of a JDBC one
		implements SqlMutationToJdbcMutationConverter {
	protected InsertToRxInsertConverter(SessionFactoryImplementor sessionFactory) {
		super( sessionFactory );
	}

	public static RxMutation createRxInsert(InsertStatement sqlAst, SessionFactoryImplementor sessionFactory) {
		final InsertToRxInsertConverter walker = new InsertToRxInsertConverter( sessionFactory );
		walker.processStatement( sqlAst );
		return null;
		/*return new JdbcInsert() {
			public boolean isKeyGenerationEnabled() {
				return false;
			}

			public String getSql() {
				return walker.getSql();
			}

			public List<JdbcParameterBinder> getParameterBinders() {
				return walker.getParameterBinders();
			}

			public Set<String> getAffectedTableNames() {
				return walker.getAffectedTableNames();
			}
		};*/
	}


	private void processStatement(InsertStatement sqlAst) {
		this.appendSql( "insert into " );
		PhysicalTable targetTable = (PhysicalTable) sqlAst.getTargetTable().getTable();
		String tableName = this.getSessionFactory()
				.getJdbcServices()
				.getJdbcEnvironment()
				.getQualifiedObjectNameFormatter()
				.format(
						targetTable.getQualifiedTableName(),
						this.getSessionFactory().getJdbcServices().getJdbcEnvironment().getDialect()
				);
		this.appendSql( tableName );
		this.appendSql( " (" );
		boolean firstPass = true;

		Iterator var5;
		ColumnReference columnReference;
		for ( var5 = sqlAst.getTargetColumnReferences().iterator(); var5.hasNext(); this.visitColumnReference(
				columnReference ) ) {
			columnReference = (ColumnReference) var5.next();
			if ( firstPass ) {
				firstPass = false;
			}
			else {
				this.appendSql( ", " );
			}
		}

		this.appendSql( ") values (" );
		firstPass = true;

		Expression expression;
		for ( var5 = sqlAst.getValues().iterator(); var5.hasNext(); expression.accept( this ) ) {
			expression = (Expression) var5.next();
			if ( firstPass ) {
				firstPass = false;
			}
			else {
				this.appendSql( ", " );
			}
		}

		this.appendSql( ")" );
	}
}
