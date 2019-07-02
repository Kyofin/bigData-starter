package phoenix;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 *  直接使用原生jdbc访问Phoenix
 */
public class PhoenixTest {

  public static void main(String[] args) throws Exception {
    Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
    // 写上zookeeper地址
    Connection connection = DriverManager.getConnection("jdbc:phoenix:localhost:2181");

    PreparedStatement statement = connection.prepareStatement("select * from TEST");

    ResultSet resultSet = statement.executeQuery();

    while (resultSet.next()) {
      System.out.println(resultSet.getString("MYCOLUMN"));
    }

    statement.close();
    connection.close();
  }
}
