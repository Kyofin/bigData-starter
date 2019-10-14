import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.commons.lang.StringUtils;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.util.ColumnInfo;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.Test;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @program: bigdata-starter
 * @author: huzekang
 * @create: 2019-10-11 15:09
 **/
public class PhoenixUtilTest {
    private DataSource getHikariDataSource() {
        HikariConfig jdbcConfig = new HikariConfig();
        jdbcConfig.setPoolName(getClass().getName());
        jdbcConfig.setDriverClassName("org.apache.phoenix.jdbc.PhoenixDriver");
        jdbcConfig.setJdbcUrl("jdbc:phoenix:cdh01:2181");
        jdbcConfig.setMaximumPoolSize(10);
        jdbcConfig.setMaxLifetime(1000);
        jdbcConfig.setConnectionTimeout(5000);
        jdbcConfig.setIdleTimeout(2000);

        return new HikariDataSource(jdbcConfig);
    }

    private Connection getConnection() throws ClassNotFoundException, SQLException {
//        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
//        // 写上zookeeper地址
//        Connection connection = DriverManager.getConnection("jdbc:phoenix:cdh01:2181");
        DataSource dataSource = getHikariDataSource();

        return dataSource.getConnection();
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

    /**
     * 获取主键
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    @Test
    public void getPrimaryKey() throws SQLException, ClassNotFoundException {
        Connection connection = getConnection();
        final String schema = "HZK_SCHEMA";
        final String tableName = "TESTTABLE";
        ResultSet rs = connection.prepareStatement(String.format("SELECT COLUMN_NAME,DATA_TYPE,NULLABLE,ORDINAL_POSITION,KEY_SEQ from SYSTEM.CATALOG where TABLE_SCHEM = '%s' AND TABLE_NAME = '%s'  AND KEY_SEQ = 1", schema,tableName)).executeQuery();
        while (rs.next()) {
            System.out.println(rs.getString(1));
        }

    }

    /**
     * 获取指定表的列信息
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    @Test
    public void getColumnInfoFromTable() throws SQLException, ClassNotFoundException {
        Connection connection = getConnection();
        final String schema = "HZK_SCHEMA";
        final String tableName = "TESTTABLE";
        List<ColumnInfo> columnInfos = SchemaUtil.generateColumnInfo(connection, schema + QueryConstants.NAME_SEPARATOR + tableName, null, true);
        List<ColumnInfoVO> columnInfoVOS = columnInfos.stream().map(e -> new ColumnInfoVO(e.getDisplayName(), e.getSqlType(), e.getPDataType().getSqlTypeName())).collect(Collectors.toList());
        System.out.println(columnInfoVOS);
    }



}
