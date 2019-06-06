package org.protor.sandbox;

import java.sql.*;
import java.util.Arrays;

public class Test02 {

	static final String QUERY_PREPARED_SALARY_DEPARTMENT = "select * from employees where salary > ? and department=?";
	static final String QUERY_CALLABLE_INCREASE_SALARY_DEPARTMENT = "{call increase_salaries_for_department(?,?)}";
	static final String QUERY_CALLABLE_GREET_DEPARTMENT = "{call greet_the_department(?)}";
	static final String QUERY_CALLABLE_COUNT_DEPARTMENT = "{call get_count_for_department(?,?)}";
	static final String QUERY_CALLABLE_GET_EMPLOYEES_DEPARTMENT = "{call get_employees_for_department(?)}";

	public static void main(String[] args) {
		System.out.println(">>>>> Test JDBC");

		// "jdbc:mysql://localhost/db?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"
		Connection myConn = null;
		PreparedStatement myStmt = null;
		ResultSet myRs = null;

		CallableStatement myCallStmt = null;

		if (args.length == 0) {
			System.err.println("Give root password as command line argument!\nTerminating.");
			System.exit(1);
		}
		String password = args[0];

		try {
			// 1. Get a connection to database
			myConn = DriverManager.getConnection(
					"jdbc:mysql://localhost:3306/demo_demarco", 
					"root" , password);

			// 2. Prepare statement
			myStmt = myConn.prepareStatement(Test02.QUERY_PREPARED_SALARY_DEPARTMENT);

			// 3. Set the parameters
			myStmt.setDouble(1, 80000);
			myStmt.setString(2, "Legal");

			// 4. Execute SQL query
			myRs = myStmt.executeQuery();

			// 5. Display the result set
			Test02.displayEmployees(myRs);

			//
			// Reuse the prepared statement:  salary > 25000,  department = HR
			//

			System.out.println("\n\nReuse the prepared statement:  salary > 25000,  department = HR");

			// 6. Set the parameters
			myStmt.setDouble(1, 25000);
			myStmt.setString(2, "HR");

			// 7. Execute SQL query
			myRs = myStmt.executeQuery();

			// 8. Display the result set
			displayEmployees(myRs);

			//-------------------------

			myCallStmt = myConn.prepareCall(Test02.QUERY_CALLABLE_INCREASE_SALARY_DEPARTMENT);
			myCallStmt.setString(1, "Engineering");
			myCallStmt.setDouble(2, 50000.0);

			System.out.println("Calling: " + QUERY_CALLABLE_INCREASE_SALARY_DEPARTMENT);

			myCallStmt.execute();

			System.out.println("call done.");

			//-------------------------

			myCallStmt = myConn.prepareCall(Test02.QUERY_CALLABLE_GREET_DEPARTMENT);
			
			myCallStmt.setString(1, "Legal");
			
			System.out.println("Calling: " + QUERY_CALLABLE_GREET_DEPARTMENT);
			
			myCallStmt.execute();
			String message = myCallStmt.getString(1);
			
			System.out.println(message);

			System.out.println("call done.");

			//-------------------------

			myCallStmt = myConn.prepareCall(Test02.QUERY_CALLABLE_COUNT_DEPARTMENT);
			
			myCallStmt.setString(1, "HR");
			
			System.out.println("Calling: " + QUERY_CALLABLE_COUNT_DEPARTMENT);
			
			myCallStmt.execute();
			int count = myCallStmt.getInt(2);
			System.out.println("Counted HR: " + count);

			System.out.println("call done.");

			//-------------------------

			String[] departments = new String[]{"HR", "Engineering", "Legal"};
			
			Arrays.asList(departments).stream()
				.map(d -> {
					CallableStatement c;
						try {
							Connection conn = DriverManager.getConnection(
									"jdbc:mysql://localhost:3306/demo_demarco", 
									"root" , password);
							c = conn.prepareCall(Test02.QUERY_CALLABLE_GET_EMPLOYEES_DEPARTMENT);
							c.setString(1, d);
							c.execute();
							return c.getResultSet();
						} catch (SQLException e) {
							e.printStackTrace();
							return null;
						}
				}) // Lambda function
//				.map(rs -> {
//					try {
//						return rs.getString("last_name");
//					} catch (SQLException e) {
//						e.printStackTrace();
//						return null;
//					}
//				})
//				.forEach(ln -> System.out.println("Last name: " + ln));
				.forEach(rs -> {//					try {
						displayEmployees(rs);
					} catch (SQLException e) {
						e.printStackTrace();
					}
				});

//			for (int i = 0; i < departments.length; i++) {
//				myCallStmt = myConn.prepareCall(Test02.QUERY_CALLABLE_GET_EMPLOYEES_DEPARTMENT);
//				
//				String department = departments[i];
//				myCallStmt.setString(1, department);
//				
//				System.out.println(
//						"Calling: " 
//								+ QUERY_CALLABLE_GET_EMPLOYEES_DEPARTMENT 
//								+ "<-- " + department);
//				
//				myCallStmt.execute();
//				myRs = myCallStmt.getResultSet();
//	
//				System.out.println("call done.");
//				Test02.displayEmployees(myRs);
//			}
			
			myRs.close();
			myStmt.close();
			myConn.close();

		} catch (Exception exc) {
			exc.printStackTrace();
			System.err.println("A problem occurred with SQL. Terminating.");
			System.exit(1);
		}

	}

	private static void displayEmployees(ResultSet myRs) throws SQLException {
		while (myRs.next()) {
			String lastName = myRs.getString("last_name");
			String firstName = myRs.getString("first_name");
			double salary = myRs.getDouble("salary");
			String department = myRs.getString("department");

			System.out.printf("%s, %s, %.2f, %s\n", lastName, firstName, salary, department);
		}
	}
}
