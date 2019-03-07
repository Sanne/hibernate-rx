package org.hibernate.rx.sql.exec.spi;

import java.util.List;
import java.util.Set;

public interface RxMutation {
	/**
	 * Get the SQL command we will be executing through JDBC PreparedStatement
	 * or CallableStatement
	 */
	String getSql();

	/**
	 * Get the list of parameter binders for the generated PreparedStatement
	 */
	List<RxParameterBinder> getParameterBinders();

	Set<String> getAffectedTableNames();
}
