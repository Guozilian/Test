package org.example.dao;

import java.sql.Connection;
import java.sql.DriverManager;

public class Conn {
        Connection con;
        public Connection getCon(){
            try{
                Class.forName("com.mysql.cj.jdbc.Driver");
                System.out.println("数据库驱动加载成功");
                con= DriverManager.getConnection("jdbc:mysql://localhost:3306/work?rewriteBatchedStatements=true&serverTimezone=UTC&useUnicode=true&characterEncoding=utf-8","root","G.zl314159");
                System.out.println("数据库链接成功");
                return con;
            }
            catch (Exception e){
                e.printStackTrace();
                return null;
            }
        }
}
