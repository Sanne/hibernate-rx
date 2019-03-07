package org.hibernate.rx.impl;

import java.sql.PreparedStatement;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;

import org.hibernate.HibernateException;
import org.hibernate.boot.model.domain.EntityMapping;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.id.IdentifierGenerator;
import org.hibernate.loader.internal.TemplateParameterBindingContext;
import org.hibernate.metamodel.model.creation.spi.RuntimeModelCreationContext;
import org.hibernate.metamodel.model.domain.internal.entity.SingleTableEntityTypeDescriptor;
import org.hibernate.metamodel.model.domain.spi.DiscriminatorDescriptor;
import org.hibernate.metamodel.model.domain.spi.EntityTypeDescriptor;
import org.hibernate.metamodel.model.domain.spi.IdentifiableTypeDescriptor;
import org.hibernate.metamodel.model.domain.spi.TenantDiscrimination;
import org.hibernate.metamodel.model.relational.spi.Column;
import org.hibernate.metamodel.model.relational.spi.JoinedTableBinding;
import org.hibernate.query.internal.QueryOptionsImpl;
import org.hibernate.query.spi.QueryOptions;
import org.hibernate.rx.sql.exec.spi.RxMutation;
import org.hibernate.sql.SqlExpressableType;
import org.hibernate.sql.ast.Clause;
import org.hibernate.sql.ast.consume.spi.InsertToJdbcInsertConverter;
import org.hibernate.sql.ast.produce.sqm.spi.Callback;
import org.hibernate.sql.ast.tree.spi.InsertStatement;
import org.hibernate.sql.ast.tree.spi.expression.ColumnReference;
import org.hibernate.sql.ast.tree.spi.expression.LiteralParameter;
import org.hibernate.sql.ast.tree.spi.from.TableReference;
import org.hibernate.sql.exec.spi.ExecutionContext;
import org.hibernate.sql.exec.spi.JdbcMutation;
import org.hibernate.sql.exec.spi.ParameterBindingContext;

public class RxSingleTableEntityTypeDescriptor<T> extends SingleTableEntityTypeDescriptor<T> implements
		EntityTypeDescriptor<T> {

	public RxSingleTableEntityTypeDescriptor(
			EntityMapping bootMapping,
			IdentifiableTypeDescriptor superTypeDescriptor,
			RuntimeModelCreationContext creationContext)
			throws HibernateException {
		super( bootMapping, superTypeDescriptor, creationContext );
	}

	protected Object insertInternal(
			Object id,
			Object[] fields,
			Object object,
			SharedSessionContractImplementor session) {
		// generate id if needed
		if ( id == null ) {
			final IdentifierGenerator generator = getHierarchy().getIdentifierDescriptor().getIdentifierValueGenerator();
			if ( generator != null ) {
				id = generator.generate( session, object );
			}
		}

//		final Object unresolvedId = getHierarchy().getIdentifierDescriptor().unresolve( id, session );
		final Object unresolvedId = id;
		final ExecutionContext executionContext = getExecutionContext( session );

		// for now - just root table
		// for now - we also regenerate these SQL AST objects each time - we can cache these
		executeInsert( fields, session, unresolvedId, executionContext, new TableReference( getPrimaryTable(), null, false) );

//		getSecondaryTableBindings().forEach(
//				tableBindings -> executeJoinTableInsert(
//						fields,
//						session,
//						unresolvedId,
//						executionContext,
//						tableBindings
//				)
//		);

		return id;
	}

	private ExecutionContext getExecutionContext(SharedSessionContractImplementor session) {
		return new ExecutionContext() {
			private final ParameterBindingContext parameterBindingContext = new TemplateParameterBindingContext( session.getFactory() );

			@Override
			public SharedSessionContractImplementor getSession() {
				return session;
			}

			@Override
			public QueryOptions getQueryOptions() {
				return new QueryOptionsImpl();
			}

			@Override
			public ParameterBindingContext getParameterBindingContext() {
				return parameterBindingContext;
			}

			@Override
			public Callback getCallback() {
				return afterLoadAction -> {
				};
			}
		};
	}

	private void executeInsert(
			Object[] fields,
			SharedSessionContractImplementor session,
			Object unresolvedId,
			ExecutionContext executionContext,
			TableReference tableReference) {

		final InsertStatement insertStatement = new InsertStatement( tableReference );
		// todo (6.0) : account for non-generated identifiers

		getHierarchy().getIdentifierDescriptor().dehydrate(
				// NOTE : at least according to the argument name (`unresolvedId`), the
				// 		incoming id value should already be unresolved - so do not
				// 		unresolve it again
				getHierarchy().getIdentifierDescriptor().unresolve( unresolvedId, session ),
				//unresolvedId,
				(jdbcValue, type, boundColumn) -> {
					insertStatement.addTargetColumnReference( new ColumnReference( boundColumn ) );
					insertStatement.addValue(
							new LiteralParameter(
									jdbcValue,
									boundColumn.getExpressableType(),
									Clause.INSERT,
									session.getFactory().getTypeConfiguration()
							)
					);
				},
				Clause.INSERT,
				session
		);

		final DiscriminatorDescriptor<Object> discriminatorDescriptor = getHierarchy().getDiscriminatorDescriptor();
		if ( discriminatorDescriptor != null ) {
			addInsertColumn(
					session,
					insertStatement,
					discriminatorDescriptor.unresolve( getDiscriminatorValue(), session ),
					discriminatorDescriptor.getBoundColumn(),
					discriminatorDescriptor.getBoundColumn().getExpressableType()
			);
		}

		final TenantDiscrimination tenantDiscrimination = getHierarchy().getTenantDiscrimination();
		if ( tenantDiscrimination != null ) {
			addInsertColumn(
					session,
					insertStatement,
					tenantDiscrimination.unresolve( session.getTenantIdentifier(), session ),
					tenantDiscrimination.getBoundColumn(),
					tenantDiscrimination.getBoundColumn().getExpressableType()
			);
		}

		visitStateArrayContributors(
				contributor -> {
					final int position = contributor.getStateArrayPosition();
					final Object domainValue = fields[position];
					contributor.dehydrate(
							// todo (6.0) : fix this - specifically this isInstance check is bad
							// 		sometimes the values here are unresolved and sometimes not;
							//		need a way to ensure they are always one form or the other
							//		during these calls (ideally unresolved)
							contributor.getJavaTypeDescriptor().isInstance( domainValue )
									? contributor.unresolve( domainValue, session )
									: domainValue,
							(jdbcValue, type, boundColumn) -> {
								if ( boundColumn.getSourceTable().equals( tableReference.getTable() ) ) {
									addInsertColumn( session, insertStatement, jdbcValue, boundColumn, type );
								}
							},
							Clause.INSERT,
							session
					);
				}
		);

//		executeInsert( executionContext, insertStatement );
	}

	private void executeJoinTableInsert(
			Object[] fields,
			SharedSessionContractImplementor session,
			Object unresolvedId,
			ExecutionContext executionContext,
			JoinedTableBinding tableBindings) {
		if ( tableBindings.isInverse() ) {
			return;
		}

		final TableReference tableReference = new TableReference( tableBindings.getReferringTable(), null , tableBindings.isOptional());
		final ValuesNullChecker jdbcValuesToInsert = new ValuesNullChecker();
		final InsertStatement insertStatement = new InsertStatement( tableReference );

		visitStateArrayContributors(
				contributor -> {
					final int position = contributor.getStateArrayPosition();
					final Object domainValue = fields[position];
					contributor.dehydrate(
							// todo (6.0) : fix this - specifically this isInstance check is bad
							// 		sometimes the values here are unresolved and sometimes not;
							//		need a way to ensure they are always one form or the other
							//		during these calls (ideally unresolved)
							contributor.getJavaTypeDescriptor().isInstance( domainValue )
									? contributor.unresolve( domainValue, session )
									: domainValue,
							(jdbcValue, type, boundColumn) -> {
								if ( boundColumn.getSourceTable().equals( tableReference.getTable() ) ) {
									if ( jdbcValue != null ) {
										jdbcValuesToInsert.setNotAllNull();
										addInsertColumn( session, insertStatement, jdbcValue, boundColumn, type );
									}
								}
							},
							Clause.INSERT,
							session
					);
				}
		);

		if ( jdbcValuesToInsert.areAllNull() ) {
			return;
		}

		getHierarchy().getIdentifierDescriptor().dehydrate(
				// NOTE : at least according to the argument name (`unresolvedId`), the
				// 		incoming id value should already be unresolved - so do not
				// 		unresolve it again
				getHierarchy().getIdentifierDescriptor().unresolve( unresolvedId, session ),
				//unresolvedId,
				(jdbcValue, type, boundColumn) -> {
					final Column referringColumn = tableBindings.getJoinForeignKey()
							.getColumnMappings()
							.findReferringColumn( boundColumn );
					addInsertColumn(
							session,
							insertStatement,
							jdbcValue,
							referringColumn,
							boundColumn.getExpressableType()
					);
				},
				Clause.INSERT,
				session
		);

		final TenantDiscrimination tenantDiscrimination = getHierarchy().getTenantDiscrimination();
		if ( tenantDiscrimination != null ) {
			addInsertColumn(
					session,
					insertStatement,
					tenantDiscrimination.unresolve( session.getTenantIdentifier(), session ),
					tenantDiscrimination.getBoundColumn(),
					tenantDiscrimination.getBoundColumn().getExpressableType()
			);
		}

		executeOperation( executionContext, insertStatement );
	}

	private CompletionStage<?> executeOperation(ExecutionContext executionContext, InsertStatement insertStatement) {
		RxMutation mutation = InsertToRxInsertConverter.createRxInsert( insertStatement, executionContext.getSession().getSessionFactory() );
		RxMutationExecutor executor = new RxMutationExecutor();
		return executor.execute(
				mutation,
				executionContext );
	}

//
//	private void executeInsert(ExecutionContext executionContext, InsertStatement insertStatement) {
//		JdbcMutation jdbcInsert = InsertToJdbcInsertConverter.createJdbcInsert(
//				insertStatement,
//				executionContext.getSession().getSessionFactory()
//		);
//		executeOperation( executionContext, jdbcInsert, (rows, prepareStatement) -> {} );
//	}



	private void addInsertColumn(
			SharedSessionContractImplementor session,
			InsertStatement insertStatement,
			Object jdbcValue,
			Column referringColumn,
			SqlExpressableType expressableType) {
		if ( jdbcValue != null ) {
			insertStatement.addTargetColumnReference( new ColumnReference( referringColumn ) );
			insertStatement.addValue(
					new LiteralParameter(
							jdbcValue,
							expressableType,
							Clause.INSERT,
							session.getFactory().getTypeConfiguration()
					)
			);
		}
	}

	private class ValuesNullChecker {
		private boolean allNull = true;

		private void setNotAllNull(){
			allNull = false;
		}

		public boolean areAllNull(){
			return allNull;
		}
	}
}
