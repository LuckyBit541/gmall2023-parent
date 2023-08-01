package org.lxk.gmall.realtime.util;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.flink.shaded.guava30.com.google.common.base.CaseFormat;
import org.lxk.gmall.realtime.common.GmallConstant;

import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class JDBCUtil {

    public static Connection getJDBCConnection() {
        try {
            // 1 加载驱动
            Class.forName(GmallConstant.JDBC_DRIVER);

            // 2 获取连接
            Connection connection = DriverManager.getConnection(GmallConstant.JDB_URL,"root","aaaaaa");
            return connection;
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @param con
     * @param sql
     * @param parasInSql
     * @param ac
     * @param isMatchColumnToCamel
     * @param <T>
     * @return
     */

    public static <T> List<T> qeuryList(Connection con, String sql, Object[] parasInSql, Class<T> ac, boolean isMatchColumnToCamel) {
        ResultSet resultSet;
        List list;
        try {

            // 1 查询
            PreparedStatement ps = con.prepareStatement(sql);
            for (int i = 1; i <= parasInSql.length; i++) {
                ps.setObject(i, parasInSql[i - 1]);
            }
            resultSet = ps.executeQuery();
            // 2 将查询结果封装到集合中

            ResultSetMetaData metaData = resultSet.getMetaData();
            list = new ArrayList<T>();

            while (resultSet.next()) {

                T t = ac.newInstance();
                for (int i = 1; i <= metaData.getColumnCount(); i++) {

                    String columnLabel = metaData.getColumnLabel(i);
                    Object object = resultSet.getObject(i);
                    if (isMatchColumnToCamel) {
                        columnLabel = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnLabel);
                    }
                    BeanUtils.setProperty(t, columnLabel, object);
                }
                BeanUtils.setProperty(t, "op", "r");
                list.add(t);


            }

        } catch (InvocationTargetException e) {
            throw new RuntimeException(e);
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException | SQLException e) {
            throw new RuntimeException(e);
        }
        return list;
    }

    public static void closeConnection(Connection jdbcConnection) {

        try {
            jdbcConnection.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
