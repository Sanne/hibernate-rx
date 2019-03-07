package org.hibernate.rx;

import java.util.Objects;
import java.util.Random;
import javax.persistence.Entity;
import javax.persistence.Id;

import org.hibernate.boot.MetadataSources;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.AvailableSettings;
import org.hibernate.metamodel.model.creation.internal.PersisterClassResolverInitiator;
import org.hibernate.rx.impl.RxSingleTableEntityTypeDescriptor;
import org.hibernate.rx.service.RxRuntimeModelDescriptorResolver;

import org.hibernate.testing.junit5.SessionFactoryBasedFunctionalTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;

import static org.assertj.core.api.Assertions.assertThat;

@Timeout( 60_000 ) // 1 H, I need to debug
@ExtendWith(VertxExtension.class)
public class ReactiveSessionTest extends SessionFactoryBasedFunctionalTest {

	RxHibernateSession session = null;

	@BeforeEach
	public void setupSession() {
		session = getSessionFactoryProducer()
				.produceSessionFactory()
				.unwrap( RxHibernateSessionFactory.class )
				.openRxSession();
	}

	@AfterEach
	public void closeSession() {
		session.close();
	}

	@Override
	protected boolean exportSchema() {
		return true;
	}

	@Override
	protected void applySettings(StandardServiceRegistryBuilder builer) {
		// TODO: Move this somewhere else in the implementation
		builer.applySetting( PersisterClassResolverInitiator.IMPL_NAME, RxRuntimeModelDescriptorResolver.class.getName() );
		builer.applySetting( AvailableSettings.DIALECT, "org.hibernate.dialect.PostgreSQL9Dialect" );
		builer.applySetting( AvailableSettings.DRIVER, "org.postgresql.Driver" );
		builer.applySetting( AvailableSettings.USER, "hibernate-rx" );
		builer.applySetting( AvailableSettings.PASS, "hibernate-rx" );
		builer.applySetting( AvailableSettings.URL, "jdbc:postgresql://localhost:5432/hibernate-rx" );
	}

	@Override
	protected void applyMetadataSources(MetadataSources metadataSources) {
		metadataSources.addAnnotatedClass( GuineaPig.class );
	}

	@Test
	public void testRegularPersist() {
		sessionFactoryScope().inTransaction( (session) -> {
			session.persist( new GuineaPig( 2, "Aloi" ) );
		} );
		System.out.println( "Wow!" );
	}

	@Test
	public void testRegularFind() {
		sessionFactoryScope().inTransaction( (session) -> {
			session.persist( new GuineaPig( 2, "Aloi" ) );
		} );
		sessionFactoryScope().inTransaction( (session) -> {
			GuineaPig guineaPig = session.find( GuineaPig.class, 2 );
			System.out.println( "Wow!" );
		} );
	}


	@Test
	public void testReactivePersist(VertxTestContext testContext) {
		final GuineaPig mibbles = new GuineaPig( 22, "Mibbles" );

		RxSession rxSession = session.getRxSession();
		rxSession.inTransaction( (rx, tx) -> {
			rx.persist( mibbles )
					.whenComplete( (pig, err) -> {
						try {
							tx.commit();
							assertThat( err ).isNull();
							testContext.completeNow();
						}
						catch (Throwable t) {
							testContext.failNow( t );
							tx.rollback();
						}
					} );
		} );
	}

	@Test
	public void testReactivePersistAndThenFind(VertxTestContext testContext) {
		final GuineaPig mibbles = new GuineaPig( 22, "Mibbles" );

		RxSession rxSession = session.getRxSession();
		rxSession.inTransaction( (rx, tx) -> {
			rx.persist( mibbles )
					.whenComplete( (nope, errPersist) -> {
						try {
							assertThat( nope ).isNull();
							rx.find( GuineaPig.class, 22 )
									.whenComplete( (pig, errFind) -> {
										try {
											tx.commit();
											System.out.println( "Commit" );
											assertThat( pig ).hasValue( mibbles );
											testContext.completeNow();
										}
										catch (Throwable t) {
											testContext.failNow( t );
											tx.rollback();
										}
									} );
						}
						catch (Throwable t) {
							testContext.failNow( t );
							tx.rollback();
						}
					} );
		} )
			// Transaction completed
			.whenComplete( (nope, err) -> {
				if ( err != null ) {
					testContext.failNow( err );
				}
			} );
	}

	@Entity
	public static class GuineaPig {
		@Id
		private Integer id;
		private String name;

		public GuineaPig() {
		}

		public GuineaPig(String name) {
			this.id = new Random().nextInt();
			this.name = name;
		}

		public GuineaPig(Integer id, String name) {
			this.id = id;
			this.name = name;
		}

		public Integer getId() {
			return id;
		}

		public void setId(Integer id) {
			this.id = id;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		@Override
		public String toString() {
			return name;
		}

		@Override
		public boolean equals(Object o) {
			if ( this == o ) {
				return true;
			}
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			GuineaPig guineaPig = (GuineaPig) o;
			return Objects.equals( name, guineaPig.name );
		}

		@Override
		public int hashCode() {
			return Objects.hash( name );
		}
	}
}
