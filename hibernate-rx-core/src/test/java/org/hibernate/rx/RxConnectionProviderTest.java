package org.hibernate.rx;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.hibernate.cfg.AvailableSettings;
import org.hibernate.rx.service.RxConnection;
import org.hibernate.rx.service.RxConnectionPoolProviderImpl;
import org.hibernate.rx.service.initiator.RxConnectionPoolProvider;
import org.hibernate.service.spi.Configurable;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.reactiverse.pgclient.PgConnection;
import io.reactiverse.pgclient.PgPool;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(VertxExtension.class)
public class RxConnectionProviderTest {

	protected Map<String, Object> settings() {
		Map<String, Object> configuration = new HashMap<>();
		configuration.put( AvailableSettings.USER, "hibernate-rx" );
		configuration.put( AvailableSettings.PASS, "hibernate-rx" );
		configuration.put( AvailableSettings.DATASOURCE, "hibernate-rx" );
		configuration.put( AvailableSettings.URL, "jdbc:postgresql://localhost:5432/hibernate-rx" );
		return Collections.unmodifiableMap( configuration );

	}

	@Test
	public void connectSuccessfully(VertxTestContext context) {
		try {
			RxConnectionPoolProvider provider = new RxConnectionPoolProviderImpl();
			( (Configurable) provider ).configure( settings() );
			RxConnection rxConn = provider.getConnection();
			rxConn.unwrap( PgPool.class ).getConnection( ar1 -> {
				PgConnection pgConnection = null;
				try {
					assertThat( ar1.succeeded() ).isTrue();
					pgConnection = ar1.result();
					context.completeNow();
				}
				catch (Throwable t) {
					context.failNow( t );
				}
				finally {
					if ( pgConnection != null ) {
						pgConnection.close();
					}
				}
			} );
		}
		catch (Throwable t) {
			context.failNow( t );
		}
	}

}
