import org.apache.commons.lang.StringUtils;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * @program: bigdata-starter
 * @author: huzekang
 * @create: 2019-10-11 15:09
 **/
public class PhoenixUtilTest {

    private Connection getConnection() throws ClassNotFoundException, SQLException {
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        // 写上zookeeper地址
        Connection connection = DriverManager.getConnection("jdbc:phoenix:cdh01:2181");
        return connection;
    }

    @Test
    public void checkConnection() throws ClassNotFoundException, SQLException {

        Connection connection = getConnection();
        if (Objects.nonNull(connection)) {
            try {
                connection.close();
            } catch (SQLException e) {
                System.out.println("error get connection");
            }
        }
    }

    @Test
    public void getDatabaseList() throws SQLException, ClassNotFoundException {
        Connection connection = getConnection();
        List dbList = new ArrayList();
        ResultSet rs = connection.prepareStatement("SELECT DISTINCT TABLE_SCHEM from SYSTEM.CATALOG").executeQuery();
        while (rs.next()) {
            String dbName = rs.getString("TABLE_SCHEM");
            if (StringUtils.isEmpty(dbName)) {
                continue;
            }
            dbList.add(dbName);
        }
        System.out.println(dbList);

    }

    /**
     * 获取指定schema的table列表
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    @Test
    public void getTableList() throws SQLException, ClassNotFoundException {
        Connection connection = getConnection();
        final String schema = "HZK_SCHEMA";
        List tableList = new ArrayList();
        ResultSet rs = connection.prepareStatement(String.format("SELECT DISTINCT TABLE_NAME from SYSTEM.CATALOG where TABLE_SCHEM = '%s'", schema)).executeQuery();
        while (rs.next()) {
            String tableName = rs.getString("TABLE_NAME");
            if (StringUtils.isBlank(tableName)) {
                continue;
            }
            tableList.add(tableName);
        }
        System.out.println(tableList);

    }

    @Test
    public void getPrimaryKey() throws SQLException, ClassNotFoundException {
        Connection connection = getConnection();
        final String schema = "HZK_SCHEMA";
        final String tableName = "HZK_SCHEMA";
        ResultSet rs = connection.getMetaData().getPrimaryKeys(null, schema, tableName);
        while (rs.next()) {
            System.out.println(rs.getString(1));
        }
    }



}
