import java.sql.*;

/**
 * Created by alex on 21.12.16.
 */
public class JDBCTest {
    public static void init() throws ClassNotFoundException {
        Class.forName("org.h2.Driver");
    }

    public static Connection getConnection() throws SQLException {
        return DriverManager.getConnection("jdbc:h2:~/test", "alex", "dozori35");
    }

    public static void createEmpoyeeTable(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("create table employee (id INT, name varchar(25))");
            statement.execute("insert into employee(id, name) values(1, 'John Dow'),(2, 'Lee Chen Kwan'), (3, 'Saiyd inb Zeila')");
            statement.execute("insert into employee(id, name) values(4, 'Lynyrd Skynyrd'),(5, 'Radnar Lodbrok')");
        }
    }

    public static void addToEmployeeWithPreparedStatesment(Connection connection) throws SQLException {
        try (PreparedStatement statement = connection
                .prepareStatement("insert into employee(id,name) values(?,?)")) {
            statement.setInt(1, 6);
            statement.setString(2, "Toyotomi Hideyoshi");
            statement.addBatch();
            statement.setInt(1, 7);
            statement.setString(2, "Maharana Pratap Singh");
            statement.addBatch();
            statement.executeBatch();
        }
    }

    public static void createSalaryTable(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("create table salary (id INT auto_increment , date DATE, value FLOAT, emp_id INT )");
            statement.execute("insert into salary(date, value, emp_id) values('2016-12-12', 3500.45, 1)," +
                    "('2016-11-10', 3454.5, 1), ('2016-11-11', 2134.54, 2), ('2016-12-13', 1234.4, 3)");
            statement.execute("insert into salary(date, value, emp_id) values('2016-01-23', 2500.4, 4)," +
                    "('2016-11-10', 454.5, 5), ('2016-11-11', 2534.11, 4), ('2016-07-13', 1934.4, 2)");
        }
    }

    public static void resultSetEmployee(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            ResultSet rs = statement.executeQuery("select * from employee");
            while (rs.next()) {
                System.out.println(rs.getInt("id") + " : " + rs.getString("name"));
            }
            System.out.println("----------------");
        }
    }

    public static void resultSetSalary(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            ResultSet rs = statement.executeQuery("select * from salary");
            while (rs.next()) {
                System.out.println(rs.getInt("id") + " : " + rs.getDate("date") + ":" + rs.getDouble("value") + ":" + rs.getInt("emp_id"));
            }
            System.out.println("----------------");
        }
    }

    public static void selectEmployeesWithTotalSalary(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            ResultSet rs = statement.executeQuery("select EMPLOYEE.ID, EMPLOYEE.NAME," +
                    " SUM( select value where EMP_ID = EMPLOYEE.ID ) as Sum " +
                    "from EMPLOYEE, SALARY group by Employee.id");
            while (rs.next()) {
                System.out.println(rs.getInt("id") + ":" + rs.getString("name") + " - " + rs.getDouble("sum"));
            }
            System.out.println("----------------");
        }
    }

    public static void deleteTable(Connection connection, String table) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("drop table " + table);
        }
    }

    public static void main(String[] args) {
        try {
            init();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        try {
            try (Connection connection = getConnection()) {
                createEmpoyeeTable(connection);
                resultSetEmployee(connection);

                createSalaryTable(connection);
                resultSetSalary(connection);

                addToEmployeeWithPreparedStatesment(connection);
                resultSetEmployee(connection);

                selectEmployeesWithTotalSalary(connection);

                deleteTable(connection, "employee");
                deleteTable(connection, "salary");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}

