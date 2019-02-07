package org.hibernate.rx;

import java.util.Objects;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Persistence;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(VertxExtension.class)
public class ReactiveSessionTest {

	private RxHibernateSessionFactory rxFactory;

	@BeforeEach
	public void createFactory() {
		rxFactory = Persistence
				.createEntityManagerFactory( "ReactiveFactory" )
				.unwrap( RxHibernateSessionFactory.class );
	}

	@AfterEach
	public void closeFactory() {
		rxFactory.close();
	}

	@Test
	public void testReactiveSessionCreation() {
			RxHibernateSession session = rxFactory.openRxSession();
			RxSession rxSession = session.getRxSession();
			session.close();
			assertThat( rxSession ).as( "Reactive session not created" ).isNotNull();
	}

	@Test
	public void testReactivePersist(VertxTestContext testContext) {
		RxHibernateSession session = rxFactory.openRxSession();
		RxSession rxSession = session.getRxSession();
		rxSession.persist( new GuineaPig( "Mibbles" ) )
				.whenComplete( (pig, err) -> {
					assertThat( session.isOpen() ).isTrue();
					if ( err != null ) {
						testContext.failNow( err );
					}
					try {
						assertThat( rxSession ).isNotNull();
						testContext.completeNow();
					}
					catch (Throwable t) {
						testContext.failNow( t );
					}
				} );
	}

	@Entity
	public static class GuineaPig {
		@Id
		@GeneratedValue
		private Integer id;
		private String name;

		public GuineaPig() {
		}

		public GuineaPig(String name) {
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
