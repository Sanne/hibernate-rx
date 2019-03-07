package org.hibernate.rx.impl;

import java.util.concurrent.CompletionStage;

import org.hibernate.rx.service.RxConnection;
import org.hibernate.rx.service.initiator.RxConnectionPoolProvider;
import org.hibernate.rx.sql.exec.spi.RxMutation;
import org.hibernate.rx.sql.exec.spi.RxParameterBinder;
import org.hibernate.sql.exec.spi.ExecutionContext;

import io.reactiverse.pgclient.PgConnection;
import io.reactiverse.pgclient.PgPool;
import io.reactiverse.pgclient.PgPreparedQuery;

public class RxMutationExecutor {

	public CompletionStage<?> execute(RxMutation operation, ExecutionContext executionContext) {
		RxConnectionPoolProvider poolProvider = executionContext.getSession()
				.getSessionFactory()
				.getServiceRegistry()
				.getService( RxConnectionPoolProvider.class );

		RxConnection connection = poolProvider.getConnection();
		connection.unwrap( PgPool.class ).getConnection( ar1 -> {
			PgConnection pgConnection = ar1.result();
			pgConnection.prepare( operation.getSql(), (ar2) -> {

				if (ar2.succeeded() ) {
					PgPreparedQuery preparedQuery = ar2.result();
					int paramBindingPosition = 1;
					for ( RxParameterBinder parameterBinder : operation.getParameterBinders() ) {
						paramBindingPosition += parameterBinder.bindParameterValue(
								preparedQuery,
								paramBindingPosition,
								executionContext
						);
					}
					preparedQuery.execute( (queryResult) -> {
						System.out.println( queryResult.result() );
						pgConnection.close();
					} );
				}

				//				int rows = ps.executeUpdate();
//				expectationCkeck.accept( rows, preparedStatement );
			} );


		} );
		return null;
	}
}
