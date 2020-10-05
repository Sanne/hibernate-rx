/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.id;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.SequenceGenerator;
import javax.persistence.Table;

import org.hibernate.cfg.Configuration;
import org.hibernate.event.spi.EventSource;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.reactive.BaseReactiveTest;
import org.hibernate.reactive.session.ReactiveConnectionSupplier;
import org.hibernate.reactive.stage.Stage;

import org.junit.Test;

import io.vertx.ext.unit.TestContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.reactive.util.impl.CompletionStages.completedFuture;
import static org.hibernate.reactive.util.impl.CompletionStages.loop;

/**
 * Test that the generation of sequences using the table strategy is thread safe.
 */
public class SequenceIdGenerationTest extends BaseReactiveTest {

	protected static boolean RUN_FULL_TESTS = Boolean.getBoolean( "reactive.runFullStressTests" );
	protected static int PARALLEL_THREADS = Runtime.getRuntime().availableProcessors();

	protected static int NUMBER_OF_TASKS = PARALLEL_THREADS * 3;

	protected static int ID_GENERATED_PER_TASK = RUN_FULL_TESTS ? 100_000 : 10;

	private static final String SEQUENCE_NAME = "IdGeneratorSequence";
	private static final int INITIAL_VALUE = 13;
	private static final int ALLOCATION_SIZE = 10;

	private final CountDownLatch endGate = new CountDownLatch( NUMBER_OF_TASKS );
	private final java.util.concurrent.ExecutorService executorService = java.util.concurrent.Executors.newFixedThreadPool(
			NUMBER_OF_TASKS );

	@org.junit.After
	public void stopThreadPool() {
		executorService.shutdown();
	}

	@Test
	public void testThreadSafetyIdGeneration(TestContext context) {
		test( context, getSessionFactory().withTransaction( (session, transaction) -> {
				  IdGenerationTask[] runJobs = runJobs( session );
				  return completedFuture( runJobs );
			  } ).thenAccept( runJobs -> {
				  // Collect all the generated ids
				  final long[] allGeneratedValues = new long[ID_GENERATED_PER_TASK * NUMBER_OF_TASKS];
				  int index = 0;
				  for ( IdGenerationTask job : runJobs ) {
					  long[] generatedValues = job.retrieveAllGeneratedValues();
					  for ( int i = 0; i < generatedValues.length; i++ ) {
						  allGeneratedValues[index++] = generatedValues[i];
					  }
				  }

				  // Sort them
				  Arrays.sort( allGeneratedValues );
//				  System.out.println("-------- Sorted id generated:");
//				  Arrays.stream( allGeneratedValues )
//						  .forEach( System.out::println );

				  // Check that the first value matches the initial value;
				  // this is also important to implicitly check we produced the number of expected identifiers,
				  // by excluding any zero.
				  assertThat( allGeneratedValues[0] ).isEqualTo( INITIAL_VALUE );

				  // Check the expected values have been generated are all unique
				  for ( int k = 0; k < (allGeneratedValues.length -1) ; k++ ) {
				  	assertThat( allGeneratedValues[k] )
							  .as( "index: " + k )
							  .isLessThan( allGeneratedValues[k+1] );
				  }
			  } )
		);
	}

	protected IdGenerationTask[] runJobs(final Stage.Session session) {
		final EntityWithId entity = new EntityWithId();
		final IdGenerationTask[] runJobs = new IdGenerationTask[NUMBER_OF_TASKS];

		System.out.println( "Starting stress tests on " + PARALLEL_THREADS + " Threads running " + NUMBER_OF_TASKS + " tasks" );
		ReactiveConnectionSupplier supplier = session.unwrap( ReactiveConnectionSupplier.class );
		final ReactiveIdentifierGenerator<Long> identifierGenerator = identifierGenerator( session, EntityWithId.class, entity );

		// Prepare all jobs (quite a lot of array allocations):
		for ( int i = 0; i < NUMBER_OF_TASKS; i++ ) {
			runJobs[i] = new IdGenerationTask( supplier, identifierGenerator );
		}


		// Start them, pretty much in parallel (not really, but we have a lot so they will eventually run in parallel):
		for ( int i = 0; i < NUMBER_OF_TASKS; i ++ ) {
			executorService.submit( runJobs[i] );
		}

		try {
			//TODO setup a timeout
			endGate.await();
		}
		catch (InterruptedException e) {
			throw new IllegalStateException("Timed out!");
		}
		executorService.shutdownNow();
		return runJobs;
	}

	private static <T> ReactiveIdentifierGenerator<Long> identifierGenerator(Stage.Session session, Class<T> entityClass, T entity) {
		try {
			EventSource source = session.unwrap( EventSource.class );
			EntityPersister entityPersister = source.getEntityPersister( entityClass.getName(), entity );
			@SuppressWarnings("unchecked")
			ReactiveIdentifierGenerator<Long> identifierGenerator = (ReactiveIdentifierGenerator<Long>) entityPersister.getIdentifierGenerator();
			return identifierGenerator;
		}
		catch (Exception ex) {
			throw new RuntimeException( ex );
		}
	}

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.addAnnotatedClass( EntityWithId.class );
		return configuration;
	}

	/**
	 * When the task starts it generates several ids.
	 * @see #ID_GENERATED_PER_TASK
	 */
	class IdGenerationTask implements Runnable {

		private final long[] generatedValues = new long[ID_GENERATED_PER_TASK];
		private volatile long[] generatedValuesPublished;
		private final ReactiveConnectionSupplier supplier;
		private final ReactiveIdentifierGenerator<Long> identifierGenerator;

		private IdGenerationTask(
				ReactiveConnectionSupplier supplier,
				ReactiveIdentifierGenerator<Long> identifierGenerator) {
			this.supplier = supplier;
			this.identifierGenerator = identifierGenerator;
		}

		@Override
		public void run() {
			final long threadId = Thread.currentThread().getId();
			System.out.println( "Job setup, running on thread " + Thread.currentThread().getName() + " thread id: " + threadId );
			// Generate several ids
			loop( 0, ID_GENERATED_PER_TASK
						  , index -> identifierGenerator
								  .generate( supplier, null )
								  .thenAccept( id -> {
									  recordSingleResult( index, id, threadId );
								  } ) )
					.thenRun( () -> recordFinalOutput( threadId ) );
		}

		private void recordSingleResult(int index, long id, long threadId) {
			System.out.println( "Running on thread " + Thread.currentThread().getName() + " thread id: " + threadId );
			generatedValues[index] = id;
		}

		private synchronized void recordFinalOutput(long threadId) {
			System.out.println( "Finished running task of threadId: " + threadId + "thread name: " + Thread.currentThread().getName() + " content: " + Arrays.toString( generatedValues ) );
			this.generatedValuesPublished = generatedValues;
			endGate.countDown();
		}

		public synchronized long[] retrieveAllGeneratedValues() {
			return generatedValuesPublished;
		}
	}

	@Entity
	@Table(name = "ENTITY_WITH_ID")
	public static class EntityWithId {

		@Id
		@GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "sequenceIdGenerator")
		@SequenceGenerator(name = "sequenceIdGenerator"
				, sequenceName = SEQUENCE_NAME
				, initialValue = INITIAL_VALUE
				, allocationSize = ALLOCATION_SIZE
		)
		public Long id;
	}
}
