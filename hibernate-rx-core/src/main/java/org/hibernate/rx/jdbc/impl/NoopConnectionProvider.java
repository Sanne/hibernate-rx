package org.hibernate.rx.jdbc.impl;

import java.sql.Connection;
import java.sql.SQLException;

import org.hibernate.engine.jdbc.connections.spi.ConnectionProvider;
import org.hibernate.service.UnknownUnwrapTypeException;

public class NoopConnectionProvider implements ConnectionProvider {
	@Override
	public Connection getConnection() throws SQLException {
		return new NoopConnection();
	}

	@Override
	public void closeConnection(Connection conn) throws SQLException {
	}

	@Override
	public boolean supportsAggressiveRelease() {
		return true;
	}

	@Override
	public boolean isUnwrappableAs(Class unwrapType) {
		return false;
	}

	@Override
	public <T> T unwrap(Class<T> unwrapType) {
		throw new UnknownUnwrapTypeException( unwrapType );
	}
}
