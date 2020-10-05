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

	@Test
	public void testThreadSafetyIdGeneration(TestContext context) {
		test( context, getSessionFactory().withTransaction( (session, transaction) -> {
				  IdGenerationTask[] runJobs = runJobs( session );
				  return completedFuture( runJobs );
			  } ).thenAccept( runJobs -> {
				  // Collect all the generated ids
				  final int[] allGeneratedValues = new int[ID_GENERATED_PER_TASK * NUMBER_OF_TASKS];
				  int index = 0;
				  for ( IdGenerationTask job : runJobs ) {
					  int[] generatedValues = job.retrieveAllGeneratedValues();
					  for ( int i = 0; i < generatedValues.length; i++ ) {
						  allGeneratedValues[index++] = generatedValues[i];
					  }
				  }

				  // Sort them
				  Arrays.sort( allGeneratedValues );
				  System.out.println("-------- Sorted id generated:");
				  Arrays.stream( allGeneratedValues )
						  .forEach( System.out::println );

				  // Check the expected values have been generated
				  int expectedValue = INITIAL_VALUE;
				  for ( int k = 0; k < allGeneratedValues.length; k++ ) {
					  assertThat( allGeneratedValues[k] )
							  .as( "index: " + k )
							  .isEqualTo( expectedValue );
					  expectedValue += 1;
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
			runJobs[i] = new IdGenerationTask( supplier, identifierGenerator, entity );
		}

		// Start them, pretty much in parallel (not really, but we have a lot so they will eventually run in parallel):
		for ( int i = 0; i < NUMBER_OF_TASKS; i ++ ) {
			new Thread( runJobs[i] ).start();
		}

		await();
		return runJobs;
	}

	private void await() {
		try {
			endGate.await();
		}
		catch (InterruptedException e) {
			throw new RuntimeException( e );
		}
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

		//GuardedBy synchronization on IncrementJob.this :
		private final int[] generatedValues = new int[ID_GENERATED_PER_TASK];
		private final ReactiveConnectionSupplier supplier;
		private final ReactiveIdentifierGenerator<Long> identifierGenerator;
		private final Object entity;

		private IdGenerationTask(
				ReactiveConnectionSupplier supplier,
				ReactiveIdentifierGenerator<Long> identifierGenerator,
				Object entity) {
			this.supplier = supplier;
			this.identifierGenerator = identifierGenerator;
			this.entity = entity;
		}

		@Override
		public void run() {
			final long threadId = Thread.currentThread().getId();
			// Generate several ids
			loop( 0, ID_GENERATED_PER_TASK
						  , index -> identifierGenerator
								  .generate( supplier, entity )
								  .thenAccept( id -> {
									  record( threadId, index, id );
								  } ) );
		}

		private synchronized void record(long threadId, Integer index, Long id) {
			System.out.println( threadId + " record[" + index + "]: " + id );
			generatedValues[index] = id.intValue();
			endGate.countDown();
		}

		public int[] retrieveAllGeneratedValues() {
			return generatedValues;
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
