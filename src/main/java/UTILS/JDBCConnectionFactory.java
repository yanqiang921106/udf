package UTILS;

import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Created by dengjh on 2016/1/12.
 */
public class JDBCConnectionFactory {
    private final static String driver = "com.mysql.jdbc.Driver";
    private static String user = "bitmind";
    private static String passwd = "Hello1234";
//    private final static String url ="jdbc:mysql://rm-bp108o4b35jelmq7bzo.mysql.rds.aliyuncs.com:3306/mf";
    private final static String url ="jdbc:mysql://rm-bp17loanys4ci7ml0ao.mysql.rds.aliyuncs.com:3306/mf";

    private JDBCConnectionFactory(){
    }

    private static HikariDataSource ds = null;

    static {
        ds = new HikariDataSource();
        ds.setDriverClassName(driver);
        ds.setJdbcUrl(url);
        ds.setUsername(user);
        ds.setPassword(passwd);
        ds.setAutoCommit(true);
        ds.setMaxLifetime(30 * 60 * 1000);
        ds.setMaximumPoolSize(10);
        ds.setConnectionTimeout(15000);
    }

    public static Connection getConnection() {
        Connection con = null;
        try {
            con = ds.getConnection();
        } catch (SQLException e) {
            e.printStackTrace(System.out);
        }
        return con;
    }

    /*public static void main(String[] args){
        Connection conn=getConnection();
        if(conn!=null){
            System.out.println("connected");
        }
    }*/
}

