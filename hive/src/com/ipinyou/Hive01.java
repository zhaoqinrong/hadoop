package com.ipinyou;

import org.codehaus.groovy.runtime.powerassert.SourceText;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class Hive01 {
    public static void main(String[] args) throws Exception {
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        Connection conn = DriverManager.getConnection("jdbc:hive2://node05:10000/default","root","");
       String sql="select * from t_emp";
        PreparedStatement stat = conn.prepareStatement(sql);
        ResultSet set = stat.executeQuery();
        while (set.next()){
            System.out.print(set.getInt(1)+",");
            System.out.print(set.getString(2)+",");
            System.out.print(set.getInt(3)+",");
            System.out.println(set.getString(4));

        }
    }
}
