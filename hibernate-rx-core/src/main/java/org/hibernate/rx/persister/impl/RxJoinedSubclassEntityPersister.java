package org.hibernate.rx.persister.impl;

import org.hibernate.*;
import org.hibernate.cache.spi.access.EntityDataAccess;
import org.hibernate.cache.spi.access.NaturalIdDataAccess;
import org.hibernate.dialect.Dialect;
import org.hibernate.engine.OptimisticLockStyle;
import org.hibernate.engine.internal.Versioning;
import org.hibernate.engine.jdbc.batch.internal.BasicBatchKey;
import org.hibernate.engine.jdbc.spi.JdbcServices;
import org.hibernate.engine.spi.*;
import org.hibernate.internal.CoreLogging;
import org.hibernate.internal.CoreMessageLogger;
import org.hibernate.internal.util.collections.ArrayHelper;
import org.hibernate.jdbc.Expectation;
import org.hibernate.jdbc.Expectations;
import org.hibernate.loader.entity.UniqueEntityLoader;
import org.hibernate.mapping.PersistentClass;
import org.hibernate.mapping.Property;
import org.hibernate.persister.entity.JoinedSubclassEntityPersister;
import org.hibernate.persister.spi.PersisterCreationContext;
import org.hibernate.pretty.MessageHelper;
import org.hibernate.rx.impl.RxQueryExecutor;
import org.hibernate.rx.loader.entity.impl.RxBatchingEntityLoaderBuilder;
import org.hibernate.rx.util.RxUtil;
import org.hibernate.sql.Delete;
import org.hibernate.tuple.InMemoryValueGenerationStrategy;
import org.hibernate.type.Type;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

public class RxJoinedSubclassEntityPersister extends JoinedSubclassEntityPersister implements RxEntityPersister {

	private static final CoreMessageLogger LOG = CoreLogging.messageLogger( RxJoinedSubclassEntityPersister.class );

	// FIXME: This should probably be a service
	private final RxQueryExecutor queryExecutor = new RxQueryExecutor();

	private BasicBatchKey insertBatchKey;
	private BasicBatchKey deleteBatchKey;
	private BasicBatchKey updateBatchKey;

	private int[] insertParams;

	// HHH-4635: Some dialects force lobs as last values for SQL queries
	private final List<Integer> lobProperties = new ArrayList<>();

	public RxJoinedSubclassEntityPersister(
			PersistentClass persistentClass,
			EntityDataAccess cacheAccessStrategy,
			NaturalIdDataAccess naturalIdRegionAccessStrategy,
			PersisterCreationContext creationContext) throws HibernateException {
		super( persistentClass, cacheAccessStrategy, naturalIdRegionAccessStrategy, creationContext );
		Dialect dialect = dialect( creationContext );

		if ( dialect.forceLobAsLastValue() ) {
			int i = 0;
			Iterator<Property> iter = persistentClass.getPropertyClosureIterator();
			while ( iter.hasNext() ) {
				if ( iter.next().isLob() ) {
					lobProperties.add( i );
					i++;
				}
			}
		}

		insertParams = new int[getTableSpan()];
		for (int table = 0; table<getTableSpan(); table++) {
			insertParams[table] = 1;
		}

		for (int p = 0; p<getPropertySpan(); p++ ) {
			String[] writers = getPropertyColumnWriters(p);
			int table = getPropertyTableNumbers()[p];
			for (int i = 0; i < writers.length; i++) {
				if ("?".equals(writers[i])) writers[i] = "$" + insertParams[table]++;
			}
		}
	}

	private static String[] lower(String[] strings) {
		for (int i = 0; i < strings.length; i++) {
			strings[i] = strings[i].toLowerCase();
		}
		return strings;
	}

	private static String lower(String string) {
		return string==null ? null : string.toLowerCase();
	}

//	@Override
//	public String getTableAliasForColumn(String columnName, String rootAlias) {
//		return lower(super.getTableAliasForColumn(columnName, rootAlias));
//	}
//
//	@Override
//	public String getRootTableAlias(String drivingAlias) {
//		return lower(super.getRootTableAlias(drivingAlias));
//	}

	@Override
	protected String[] getIdentifierAliases() {
		return lower(super.getIdentifierAliases());
	}

	@Override
	public String[] getIdentifierAliases(String suffix) {
		return lower(super.getIdentifierAliases(suffix));
	}

	@Override
	public String[] getSubclassPropertyColumnAliases(String propertyName, String suffix) {
		return lower(super.getSubclassPropertyColumnAliases(propertyName, suffix));
	}

	@Override
	protected String[] getSubclassColumnAliasClosure() {
		return lower(super.getSubclassColumnAliasClosure());
	}

	@Override
	public String[] getPropertyAliases(String suffix, int i) {
		return lower(super.getPropertyAliases(suffix, i));
	}

	@Override
	public String getDiscriminatorAlias(String suffix) {
		return lower(super.getDiscriminatorAlias(suffix));
	}

	@Override
	protected String getDiscriminatorAlias() {
		return lower(super.getDiscriminatorAlias().toLowerCase());
	}

	private Dialect dialect(PersisterCreationContext creationContext) {
		return creationContext.getSessionFactory()
					.getServiceRegistry()
					.getService( JdbcServices.class )
					.getDialect();
	}

	@Override
	protected UniqueEntityLoader createEntityLoader(LockMode lockMode, LoadQueryInfluencers loadQueryInfluencers)
			throws MappingException {
		//FIXME add support to lock mode and loadQueryInfluencers

		return RxBatchingEntityLoaderBuilder.getBuilder( getFactory() )
				.buildLoader( this, batchSize, lockMode, getFactory(), loadQueryInfluencers );
	}


	@Override
	protected UniqueEntityLoader createEntityLoader(LockOptions lockOptions, LoadQueryInfluencers loadQueryInfluencers)
			throws MappingException {
		//FIXME add support to lock mode and loadQueryInfluencers
		return RxBatchingEntityLoaderBuilder.getBuilder( getFactory() )
				.buildLoader( this, batchSize, lockOptions, getFactory(), loadQueryInfluencers );
	}

	@Override
	protected Serializable insert(
			Object[] fields, boolean[] notNull, String sql, Object object, SharedSessionContractImplementor session)
			throws HibernateException {
		throw new UnsupportedOperationException( "Wrong method calls. Use the reactive equivalent." );
	}

	@Override
	protected void insert(
			Serializable id,
			Object[] fields,
			boolean[] notNull,
			int j,
			String sql,
			Object object,
			SharedSessionContractImplementor session) throws HibernateException {
		throw new UnsupportedOperationException( "Wrong method calls. Use the reactive equivalent." );
	}

	@Override
	public Serializable insert(
			Object[] fields, Object object, SharedSessionContractImplementor session) throws HibernateException {
		throw new UnsupportedOperationException( "Wrong method calls. Use the reactive equivalent." );
	}

	@Override
	public void insert(Serializable id, Object[] fields, Object object, SharedSessionContractImplementor session) {
		throw new UnsupportedOperationException( "Wrong method calls. Use the reactive equivalent." );
	}

	@Override
	protected void delete(
			Serializable id,
			Object version,
			int j,
			Object object,
			String sql,
			SharedSessionContractImplementor session,
			Object[] loadedState) throws HibernateException {
		throw new UnsupportedOperationException( "Wrong method calls. Use the reactive equivalent." );
	}

	@Override
	public void delete(
			Serializable id, Object version, Object object, SharedSessionContractImplementor session)
			throws HibernateException {
		throw new UnsupportedOperationException( "Wrong method calls. Use the reactive equivalent." );
	}

	@Override
	protected void updateOrInsert(
			Serializable id,
			Object[] fields,
			Object[] oldFields,
			Object rowId,
			boolean[] includeProperty,
			int j,
			Object oldVersion,
			Object object,
			String sql,
			SharedSessionContractImplementor session) throws HibernateException {
		throw new UnsupportedOperationException( "Wrong method calls. Use the reactive equivalent." );
	}

	@Override
	protected boolean update(
			Serializable id,
			Object[] fields,
			Object[] oldFields,
			Object rowId,
			boolean[] includeProperty,
			int j,
			Object oldVersion,
			Object object,
			String sql,
			SharedSessionContractImplementor session) throws HibernateException {
		throw new UnsupportedOperationException( "Wrong method calls. Use the reactive equivalent." );
	}

	@Override
	public void update(
			Serializable id,
			Object[] fields,
			int[] dirtyFields,
			boolean hasDirtyCollection,
			Object[] oldFields,
			Object oldVersion,
			Object object,
			Object rowId,
			SharedSessionContractImplementor session) throws HibernateException {
		throw new UnsupportedOperationException( "Wrong method calls. Use the reactive equivalent." );
	}

	private void preInsertInMemoryValueGeneration(
			Object[] fields,
			Object object,
			SharedSessionContractImplementor session) {
		if ( getEntityMetamodel().hasPreInsertGeneratedValues() ) {
			final InMemoryValueGenerationStrategy[] strategies = getEntityMetamodel().getInMemoryValueGenerationStrategies();
			for ( int i = 0; i < strategies.length; i++ ) {
				if ( strategies[i] != null && strategies[i].getGenerationTiming().includesInsert() ) {
					fields[i] = strategies[i].getValueGenerator().generateValue( (Session) session, object );
					setPropertyValue( object, i, fields[i] );
				}
			}
		}
	}

	public CompletionStage<?> insertRx(
			Serializable id,
			Object[] fields,
			Object object,
			SharedSessionContractImplementor session) {
		// apply any pre-insert in-memory value generation
		preInsertInMemoryValueGeneration( fields, object, session );

		CompletionStage<?> insertStage = RxUtil.nullFuture();
		final int span = getTableSpan();
		if ( getEntityMetamodel().isDynamicInsert() ) {
			// For the case of dynamic-insert="true", we need to generate the INSERT SQL
			boolean[] notNull = getPropertiesToInsert( fields );
			for ( int j = 0; j < span; j++ ) {
				int jj = j;
				insertStage = insertStage.thenCompose( v->
						insertRx( id, fields, notNull, jj, generateInsertString( notNull, jj ), object, session ));
			}
		}
		else {
			// For the case of dynamic-insert="false", use the static SQL
			for ( int j = 0; j < span; j++ ) {
				int jj = j;
				insertStage = insertStage.thenCompose( v->
						insertRx(
								id,
								fields,
								getPropertyInsertability(),
								jj,
								getSQLInsertStrings()[jj],
								object,
								session
						));
			}
		}
		return insertStage;
	}

	// Should it return the id?
	public CompletionStage<?> insertRx(Object[] fields, Object object, SharedSessionContractImplementor session)
			throws HibernateException {
		// apply any pre-insert in-memory value generation
		preInsertInMemoryValueGeneration( fields, object, session );

		CompletionStage<?> insertStage = RxUtil.nullFuture();
		final int span = getTableSpan();
		final Serializable id;
		if ( getEntityMetamodel().isDynamicInsert() ) {
			// For the case of dynamic-insert="true", we need to generate the INSERT SQL
			boolean[] notNull = getPropertiesToInsert( fields );
			id = insert( fields, notNull, generateInsertString( true, notNull ), object, session );
			for ( int j = 1; j < span; j++ ) {
				int jj = j;
				insertStage = insertStage.thenCompose( v->
						insertRx( id, fields, notNull, jj, generateInsertString( notNull, jj ), object, session ));
			}
		}
		else {
			// For the case of dynamic-insert="false", use the static SQL
			id = insert( fields, getPropertyInsertability(), getSQLIdentityInsertString(), object, session );
			for ( int j = 1; j < span; j++ ) {
				int jj = j;
				insertStage = insertStage.thenCompose( v->
						insertRx(
								id,
								fields,
								getPropertyInsertability(),
								jj,
								getSQLInsertStrings()[jj],
								object,
								session
						));
			}
		}
		return insertStage;
	}

	public CompletionStage<?> insertRx(
			Serializable id,
			Object[] fields,
			boolean[] notNull,
			int j,
			String sql,
			Object object,
			SharedSessionContractImplementor session) throws HibernateException {

		if ( isInverseTable( j ) ) {
			return RxUtil.nullFuture();
		}

		//note: it is conceptually possible that a UserType could map null to
		//	  a non-null value, so the following is arguable:
		if ( isNullableTable( j ) && isAllNull( fields, j ) ) {
			return RxUtil.nullFuture();
		}

		if ( LOG.isTraceEnabled() ) {
			LOG.tracev( "Inserting entity: {0}", MessageHelper.infoString( this, id, getFactory() ) );
			if ( j == 0 && isVersioned() ) {
				LOG.tracev( "Version: {0}", Versioning.getVersion( fields, this ) );
			}
		}

		// TODO : shouldn't inserts be Expectations.NONE?
		final Expectation expectation = Expectations.appropriateExpectation( insertResultCheckStyles[j] );
		final int jdbcBatchSizeToUse = session.getConfiguredJdbcBatchSize();
		final boolean useBatch = expectation.canBeBatched() &&
				jdbcBatchSizeToUse > 1 &&
				getIdentifierGenerator().supportsJdbcBatchInserts();

		if ( useBatch && insertBatchKey == null ) {
			insertBatchKey = new BasicBatchKey(
					getEntityName() + "#INSERT",
					expectation
			);
		}
		final boolean callable = isInsertCallable( j );

		PreparedStatementAdapter adapter = new PreparedStatementAdapter();
		try {
			dehydrate( null, fields, notNull, getPropertyColumnInsertable(), j, adapter, session, false );
		}
		catch (SQLException e) {
			throw new HibernateException( "Error" );
		}
		return queryExecutor.update( id, sql, adapter.getParametersAsArray(), getFactory() );
	}

	protected CompletionStage<?> deleteRx(
			Serializable id,
			Object version,
			int j,
			Object object,
			String sql,
			SharedSessionContractImplementor session,
			Object[] loadedState) throws HibernateException {

		if ( isInverseTable( j ) ) {
			return RxUtil.nullFuture();
		}
		final boolean useVersion = j == 0 && isVersioned();
		final boolean callable = isDeleteCallable( j );
		final Expectation expectation = Expectations.appropriateExpectation( deleteResultCheckStyles[j] );
		final boolean useBatch = j == 0 && isBatchable() && expectation.canBeBatched();
		if ( useBatch && deleteBatchKey == null ) {
			deleteBatchKey = new BasicBatchKey(
					getEntityName() + "#DELETE",
					expectation
			);
		}

		if ( LOG.isTraceEnabled() ) {
			LOG.tracev( "Deleting entity: {0}", MessageHelper.infoString( this, id, getFactory() ) );
			if ( useVersion ) {
				LOG.tracev( "Version: {0}", version );
			}
		}

		if ( isTableCascadeDeleteEnabled( j ) ) {
			if ( LOG.isTraceEnabled() ) {
				LOG.tracev( "Delete handled by foreign key constraint: {0}", getTableName( j ) );
			}
			//EARLY EXIT!
			return RxUtil.nullFuture();
		}

		//Render the SQL query
		PreparedStatementAdapter delete = new PreparedStatementAdapter();
		try {
			// FIXME: This is a hack to set the right type for the parameters
			//        until we have a proper type system in place
			int index = 1;

			index += expectation.prepare( delete );

			// Do the key. The key is immutable so we can use the _current_ object state - not necessarily
			// the state at the time the delete was issued
			getIdentifierType().nullSafeSet( delete, id, index, session );
			index += getIdentifierColumnSpan();

			// We should use the _current_ object state (ie. after any updates that occurred during flush)
			if ( useVersion ) {
				getVersionType().nullSafeSet( delete, version, index, session );
			}
			else if ( isAllOrDirtyOptLocking() && loadedState != null ) {
				boolean[] versionability = getPropertyVersionability();
				Type[] types = getPropertyTypes();
				for ( int i = 0; i < getEntityMetamodel().getPropertySpan(); i++ ) {
					if ( isPropertyOfTable( i, j ) && versionability[i] ) {
						// this property belongs to the table and it is not specifically
						// excluded from optimistic locking by optimistic-lock="false"
						boolean[] settable = types[i].toColumnNullness( loadedState[i], getFactory() );
						types[i].nullSafeSet( delete, loadedState[i], index, settable, session );
						index += ArrayHelper.countTrue( settable );
					}
				}
			}
		}
		catch ( SQLException e) {
			throw new HibernateException( e );
		}

		return queryExecutor.update( sql, delete.getParametersAsArray(), getFactory() );
	}

	public CompletionStage<?> deleteRx(
			Serializable id, Object version, Object object, SharedSessionContractImplementor session)
			throws HibernateException {
		final int span = getTableSpan();
		boolean isImpliedOptimisticLocking = !getEntityMetamodel().isVersioned() && isAllOrDirtyOptLocking();
		Object[] loadedState = null;
		if ( isImpliedOptimisticLocking ) {
			// need to treat this as if it where optimistic-lock="all" (dirty does *not* make sense);
			// first we need to locate the "loaded" state
			//
			// Note, it potentially could be a proxy, so doAfterTransactionCompletion the location the safe way...
			final EntityKey key = session.generateEntityKey( id, this );
			final PersistenceContext persistenceContext = session.getPersistenceContextInternal();
			Object entity = persistenceContext.getEntity( key );
			if ( entity != null ) {
				EntityEntry entry = persistenceContext.getEntry( entity );
				loadedState = entry.getLoadedState();
			}
		}

		final String[] deleteStrings;
		if ( isImpliedOptimisticLocking && loadedState != null ) {
			// we need to utilize dynamic delete statements
			deleteStrings = generateSQLDeleteStrings( loadedState );
		}
		else {
			// otherwise, utilize the static delete statements
			deleteStrings = getSQLDeleteStrings();
		}

		CompletionStage<?> deleteStage = RxUtil.nullFuture();
		for ( int j = span - 1; j >= 0; j-- ) {
			// For now we assume there is only one delete query
			int jj = j;
			Object[] state = loadedState;
			deleteStage = deleteStage.thenCompose( v->
					deleteRx( id, version, jj, object, deleteStrings[jj], session, state ));
		}

		return deleteStage;
	}

	private boolean isAllOrDirtyOptLocking() {
		return getEntityMetamodel().getOptimisticLockStyle() == OptimisticLockStyle.DIRTY
				|| getEntityMetamodel().getOptimisticLockStyle() == OptimisticLockStyle.ALL;
	}

	private String[] generateSQLDeleteStrings(Object[] loadedState) {
		int span = getTableSpan();
		String[] deleteStrings = new String[span];
		for ( int j = span - 1; j >= 0; j-- ) {
			Delete delete = new Delete()
					.setTableName( getTableName( j ) )
					.addPrimaryKeyColumns( getKeyColumns( j ) );
			if ( getFactory().getSessionFactoryOptions().isCommentsEnabled() ) {
				delete.setComment( "delete " + getEntityName() + " [" + j + "]" );
			}

			boolean[] versionability = getPropertyVersionability();
			Type[] types = getPropertyTypes();
			for ( int i = 0; i < getEntityMetamodel().getPropertySpan(); i++ ) {
				if ( isPropertyOfTable( i, j ) && versionability[i] ) {
					// this property belongs to the table and it is not specifically
					// excluded from optimistic locking by optimistic-lock="false"
					String[] propertyColumnNames = getPropertyColumnNames( i );
					boolean[] propertyNullness = types[i].toColumnNullness( loadedState[i], getFactory() );
					for ( int k = 0; k < propertyNullness.length; k++ ) {
						if ( propertyNullness[k] ) {
							delete.addWhereFragment( propertyColumnNames[k] + " = $" + ( k + 1 ) );
						}
						else {
							delete.addWhereFragment( propertyColumnNames[k] + " is null" );
						}
					}
				}
			}
			deleteStrings[j] = delete.toStatementString();
		}
		return deleteStrings;
	}

	protected CompletionStage<Boolean> updateRx(
			final Serializable id,
			final Object[] fields,
			final Object[] oldFields,
			final Object rowId,
			final boolean[] includeProperty,
			final int j,
			final Object oldVersion,
			final Object object,
			final String sql,
			final SharedSessionContractImplementor session) throws HibernateException {

		final Expectation expectation = Expectations.appropriateExpectation( updateResultCheckStyles[j] );
		final int jdbcBatchSizeToUse = session.getConfiguredJdbcBatchSize();
		final boolean useBatch = expectation.canBeBatched() && isBatchable() && jdbcBatchSizeToUse > 1;
		if ( useBatch && updateBatchKey == null ) {
			updateBatchKey = new BasicBatchKey(
					getEntityName() + "#UPDATE",
					expectation
			);
		}
		final boolean callable = isUpdateCallable( j );
		final boolean useVersion = j == 0 && isVersioned();

		if ( LOG.isTraceEnabled() ) {
			LOG.tracev( "Updating entity: {0}", MessageHelper.infoString( this, id, getFactory() ) );
			if ( useVersion ) {
				LOG.tracev( "Existing version: {0} -> New version:{1}", oldVersion, fields[getVersionProperty()] );
			}
		}

		try {
			int index = 1; // starting index
//			if ( useBatch ) {
//				update = session
//						.getJdbcCoordinator()
//						.getBatch( updateBatchKey )
//						.getBatchStatement( sql, callable );
//			}
//			else {
			final PreparedStatementAdapter update = new PreparedStatementAdapter();
//			}

			try {
				index += expectation.prepare( update );

				//Now write the values of fields onto the prepared statement
				index = dehydrate(
						id,
						fields,
						rowId,
						includeProperty,
						getPropertyColumnUpdateable(),
						j,
						update,
						session,
						index,
						true
				);

				// Write any appropriate versioning conditional parameters
				if ( useVersion && getEntityMetamodel().getOptimisticLockStyle() == OptimisticLockStyle.VERSION ) {
					if ( checkVersion( includeProperty ) ) {
						getVersionType().nullSafeSet( update, oldVersion, index, session );
					}
				}
				else if ( isAllOrDirtyOptLocking() && oldFields != null ) {
					boolean[] versionability = getPropertyVersionability(); //TODO: is this really necessary????
					boolean[] includeOldField = getEntityMetamodel().getOptimisticLockStyle() == OptimisticLockStyle.ALL
							? getPropertyUpdateability()
							: includeProperty;
					Type[] types = getPropertyTypes();
					for ( int i = 0; i < getEntityMetamodel().getPropertySpan(); i++ ) {
						boolean include = includeOldField[i] &&
								isPropertyOfTable( i, j ) &&
								versionability[i]; //TODO: is this really necessary????
						if ( include ) {
							boolean[] settable = types[i].toColumnNullness( oldFields[i], getFactory() );
							types[i].nullSafeSet(
									update,
									oldFields[i],
									index,
									settable,
									session
							);
							index += ArrayHelper.countTrue( settable );
						}
					}
				}

//				if ( useBatch ) {
//					session.getJdbcCoordinator().getBatch( updateBatchKey ).addToBatch();
//					return true;
//				}
//				else {
					return queryExecutor.update( sql, update.getParametersAsArray(), getFactory() )
							.thenApply( count -> {
								return count > 0;
							} );
//					return check(
//							session.getJdbcCoordinator().getResultSetReturn().executeUpdate( update ),
//							id,
//							j,
//							expectation,
//							update
//					);
//				}
			}
			finally {
				if ( !useBatch ) {
					session.getJdbcCoordinator().getResourceRegistry().release( update );
					session.getJdbcCoordinator().afterStatementExecution();
				}
			}

		}
		catch (SQLException e) {
			throw getFactory().getSQLExceptionHelper().convert(
					e,
					"could not update: " + MessageHelper.infoString( this, id, getFactory() ),
					sql
			);
		}
	}

	public CompletionStage<?> updateRx(
			final Serializable id,
			final Object[] fields,
			int[] dirtyFields,
			final boolean hasDirtyCollection,
			final Object[] oldFields,
			final Object oldVersion,
			final Object object,
			final Object rowId,
			final SharedSessionContractImplementor session) throws HibernateException {

		// apply any pre-update in-memory value generation
		if ( getEntityMetamodel().hasPreUpdateGeneratedValues() ) {
			final InMemoryValueGenerationStrategy[] valueGenerationStrategies = getEntityMetamodel().getInMemoryValueGenerationStrategies();
			int valueGenerationStrategiesSize = valueGenerationStrategies.length;
			if ( valueGenerationStrategiesSize != 0 ) {
				int[] fieldsPreUpdateNeeded = new int[valueGenerationStrategiesSize];
				int count = 0;
				for ( int i = 0; i < valueGenerationStrategiesSize; i++ ) {
					if ( valueGenerationStrategies[i] != null && valueGenerationStrategies[i].getGenerationTiming()
							.includesUpdate() ) {
						fields[i] = valueGenerationStrategies[i].getValueGenerator().generateValue(
								(Session) session,
								object
						);
						setPropertyValue( object, i, fields[i] );
						fieldsPreUpdateNeeded[count++] = i;
					}
				}
				if ( dirtyFields != null ) {
					dirtyFields = ArrayHelper.join( dirtyFields, ArrayHelper.trim( fieldsPreUpdateNeeded, count ) );
				}
			}
		}

		//note: dirtyFields==null means we had no snapshot, and we couldn't get one using select-before-update
		//	  oldFields==null just means we had no snapshot to begin with (we might have used select-before-update to get the dirtyFields)

		final boolean[] tableUpdateNeeded = getTableUpdateNeeded( dirtyFields, hasDirtyCollection );
		final int span = getTableSpan();

		final boolean[] propsToUpdate;
		final String[] updateStrings;
		EntityEntry entry = session.getPersistenceContextInternal().getEntry( object );

		// Ensure that an immutable or non-modifiable entity is not being updated unless it is
		// in the process of being deleted.
		if ( entry == null && !isMutable() ) {
			throw new IllegalStateException( "Updating immutable entity that is not in session yet!" );
		}
		if ( ( getEntityMetamodel().isDynamicUpdate() && dirtyFields != null ) ) {
			// We need to generate the UPDATE SQL when dynamic-update="true"
			propsToUpdate = getPropertiesToUpdate( dirtyFields, hasDirtyCollection );
			// don't need to check laziness (dirty checking algorithm handles that)
			updateStrings = new String[span];
			for ( int j = 0; j < span; j++ ) {
				updateStrings[j] = tableUpdateNeeded[j] ?
						generateUpdateString( propsToUpdate, j, oldFields, j == 0 && rowId != null ) :
						null;
			}
		}
		else if ( !isModifiableEntity( entry ) ) {
			// We need to generate UPDATE SQL when a non-modifiable entity (e.g., read-only or immutable)
			// needs:
			// - to have references to transient entities set to null before being deleted
			// - to have version incremented do to a "dirty" association
			// If dirtyFields == null, then that means that there are no dirty properties to
			// to be updated; an empty array for the dirty fields needs to be passed to
			// getPropertiesToUpdate() instead of null.
			propsToUpdate = getPropertiesToUpdate(
					( dirtyFields == null ? ArrayHelper.EMPTY_INT_ARRAY : dirtyFields ),
					hasDirtyCollection
			);
			// don't need to check laziness (dirty checking algorithm handles that)
			updateStrings = new String[span];
			for ( int j = 0; j < span; j++ ) {
				updateStrings[j] = tableUpdateNeeded[j] ?
						generateUpdateString( propsToUpdate, j, oldFields, j == 0 && rowId != null ) :
						null;
			}
		}
		else {
			// For the case of dynamic-update="false", or no snapshot, we use the static SQL
			updateStrings = getUpdateStrings(
					rowId != null,
					hasUninitializedLazyProperties( object )
			);
			propsToUpdate = getPropertyUpdateability( object );
		}

		CompletionStage<?> updateStage = RxUtil.nullFuture();
		for ( int j = 0; j < span; j++ ) {
			// Now update only the tables with dirty properties (and the table with the version number)
			if ( tableUpdateNeeded[j] ) {
				// We assume there is only one table for now
				final int jj = j;
				updateStage = updateStage.thenCompose(
								v-> updateOrInsertRx(
								id,
								fields,
								oldFields,
								jj == 0 ? rowId : null,
								propsToUpdate,
								jj,
								oldVersion,
								object,
								updateStrings[jj],
								session
						));
			}
		}
		return updateStage;
	}

	protected CompletionStage<?> updateOrInsertRx(
			final Serializable id,
			final Object[] fields,
			final Object[] oldFields,
			final Object rowId,
			final boolean[] includeProperty,
			final int j,
			final Object oldVersion,
			final Object object,
			final String sql,
			final SharedSessionContractImplementor session) throws HibernateException {

		if ( !isInverseTable( j ) ) {

			if ( isNullableTable( j ) && isAllNull( oldFields, j ) && oldFields != null ) {
				// don't bother trying to update, we know there is no row there yet
				if ( !isAllNull( fields, j ) ) {
					return insertRx( id, fields, getPropertyInsertability(), j, getSQLInsertStrings()[j], object, session );
				}
			}
			else if ( isNullableTable( j ) && isAllNull( fields, j ) ) {
				// All fields are null, we can just delete the row
				return deleteRx( id, oldVersion, j, object, getSQLDeleteStrings()[j], session, null );
			}
			else {
				return updateRx( id, fields, oldFields, rowId, includeProperty, j, oldVersion, object, sql, session )
						.thenApply( updated -> {
							if ( !updated && !isAllNull( fields, j ) ) {
								// Nothing has been updated because the row isn't in the db
								// Run an insert instead
								return insertRx( id, fields, getPropertyInsertability(), j, getSQLInsertStrings()[j], object, session );
							}
							return null;
						} );
			}
		}

		// Nothing to do;
		return RxUtil.nullFuture();
	}

	private String[] getUpdateStrings(boolean byRowId, boolean lazy) {
		if ( byRowId ) {
			return lazy ? getSQLLazyUpdateByRowIdStrings() : getSQLUpdateByRowIdStrings();
		}
		else {
			return lazy ? getSQLLazyUpdateStrings() : getSQLUpdateStrings();
		}
	}

	@Override
	protected String generateInsertString(boolean identityInsert, boolean[] includeProperty, int j) {
		return super.generateInsertString( identityInsert, includeProperty, j )
				.replace("?", "$" + insertParams[j]);
	}

	@Override
	protected String generateDeleteString(int j) {
		return super.generateDeleteString(j)
				.replaceFirst("\\?", "\\$1")
				.replaceFirst("\\?", "\\$2");
	}

	/**
	 * Generate the SQL that updates a row by id (and version)
	 */
	@Override
	protected String generateUpdateString(
			final boolean[] includeProperty,
			final int j,
			final Object[] oldFields,
			final boolean useRowId) {

		return super.generateUpdateString(includeProperty, j, oldFields, useRowId)
				.replaceFirst("\\?", "\\$" + insertParams[j])
				.replaceFirst("\\?", "\\$" + (insertParams[j]+1));
	}
}
