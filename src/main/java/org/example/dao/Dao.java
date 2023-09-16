package org.example.dao;

import com.csvreader.CsvReader;
import org.example.ulit.MyUlit;

import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;
import java.util.List;

public class Dao {
    String sql;
    MyUlit myUlit = new MyUlit();
    ResultSet resultSet;
    Statement statement = null;
    PreparedStatement preparedStatement = null;
    Connection connection;

    public Dao() {
        connection = new Conn().getCon();
    }

    //测试查询
    public void selectone() throws SQLException {

        statement = connection.createStatement();
        resultSet = statement.executeQuery("Select * from test_table");
        while (resultSet.next()) {
            int aa = resultSet.getInt(1);
            String name = resultSet.getString(2);
            System.out.println("1:"+aa+",2:"+name);

        }

        resultSet.close();
        statement.close();
    }

    public void insertJzmx() throws SQLException, IOException {
        String sql1;
        connection.setAutoCommit(false);

        CsvReader csvReader = new CsvReader("E:/jzmx.csv",',', Charset.forName("UTF-8"));
        csvReader.readHeaders();
        String[] colName = csvReader.getHeaders();
        List<Integer> type=myUlit.getType(colName);
        String sql= myUlit.getInsert_tableSql("work","jzmx",colName);

        int catch_num=0;
        long allRecordNum=0;
        int i=0;
        if (statement==null)
            statement=connection.createStatement();
        if (preparedStatement==null)
            preparedStatement = connection.prepareStatement(sql);
        csvReader.setSafetySwitch(false);

        while (csvReader.readRecord()){
            i=0;
            for (String b:csvReader.getValues()){
                if (b.length()>1000)
                    b="";
                if (i>31)
                    continue;
                else {
                    switch (type.get(i)){
                        case 0:preparedStatement.setString(i+1,b);break;
                        case 1: if (b.matches("^\\d+\\.?\\d*")) preparedStatement.setFloat(i+1,Float.parseFloat(b));else preparedStatement.setNull(i+1, Types.FLOAT);break;
                        case 2:if (b.matches("\\d{4}\\/\\d{1,2}\\/\\d{1,2}.*")) preparedStatement.setTimestamp(i+1,myUlit.getTextTimestap(b));else preparedStatement.setNull(i+1,Types.TIMESTAMP);
                    }
                }
                i++;
            }
            preparedStatement.addBatch();
            allRecordNum++;
            catch_num++;
            if (catch_num>9999){
            preparedStatement.executeBatch();
            preparedStatement.clearBatch();
            sql1="UPDATE `work`.`record` SET `record_num` = '"+allRecordNum+"' WHERE (`id` = '1')";
            statement.execute(sql1);
            connection.commit();
            catch_num=0;
            }

        }
        if (catch_num!=0){
            preparedStatement.executeBatch();
            preparedStatement.clearBatch();
            sql1="UPDATE `work`.`record` SET `record_num` = '"+allRecordNum+"' WHERE (`id` = '1')";
            statement.execute(sql1);
            connection.commit();
        }


        csvReader.close();
//      statement.execute("insert into test_table(id,name) values (5,\'胡i()（）\')");
    }

    public void insertCfmx() throws SQLException, IOException {
        String sql1;
        connection.setAutoCommit(false);

        CsvReader csvReader = new CsvReader("E:/cfmx.csv",',', Charset.forName("UTF-8"));
        csvReader.readHeaders();
        String[] colName = csvReader.getHeaders();
        List<Integer> type=myUlit.getType(colName);
        String sql= myUlit.getInsert_tableSql("work","cfmx",colName);

        int catch_num=0;
        long allRecordNum=0;
        int i=0;
        if (statement==null)
            statement=connection.createStatement();
        if (preparedStatement==null)
            preparedStatement = connection.prepareStatement(sql);
        csvReader.setSafetySwitch(false);

        while (csvReader.readRecord()){
            i=0;
            for (String b:csvReader.getValues()){
                if (b.length()>255)
                    b="";
                if (i>23)
                    continue;
                else {
                    switch (type.get(i)){
                        case 0:preparedStatement.setString(i+1,b);break;
                        case 1: if (b.matches("^\\d+\\.?\\d*")) preparedStatement.setFloat(i+1,Float.parseFloat(b));else preparedStatement.setNull(i+1, Types.FLOAT);break;
                        case 2:if (b.matches("\\d{4}\\/\\d{1,2}\\/\\d{1,2}.*")) preparedStatement.setTimestamp(i+1,myUlit.getTextTimestap(b));else preparedStatement.setNull(i+1,Types.TIMESTAMP);
                    }
                }
                i++;
            }
            preparedStatement.addBatch();
            allRecordNum++;
            catch_num++;
            if (catch_num>9999){
                preparedStatement.executeBatch();
                preparedStatement.clearBatch();
                sql1="UPDATE `work`.`record` SET `record_num` = '"+allRecordNum+"' WHERE (`id` = '2')";
                statement.execute(sql1);
                connection.commit();
                catch_num=0;

            }

        }
        if (catch_num!=0){
            preparedStatement.executeBatch();
            preparedStatement.clearBatch();
            sql1="UPDATE `work`.`record` SET `record_num` = '"+allRecordNum+"' WHERE (`id` = '2')";
            statement.execute(sql1);
            connection.commit();
        }


        csvReader.close();
//      statement.execute("insert into test_table(id,name) values (5,\'胡i()（）\')");
    }
    public void creat_table(String sql1) throws SQLException {
        statement = connection.createStatement();
        statement.execute(sql1);
        statement.close();
    }

    public void drop_table(String tablename) throws SQLException {
        sql="Drop table `work`.`"+tablename+"`";
        statement = connection.createStatement();
        statement.execute(sql);
        statement.close();
    }
    public void truncate_table(String databaseAndTableName) throws SQLException {
        sql="truncate table "+databaseAndTableName;
        statement = connection.createStatement();
        statement.execute(sql);
        statement.close();
    }
    public void updateRecord(long num) throws SQLException {
        if (statement==null)
            statement = connection.createStatement();
        sql="UPDATE `work`.`record` SET `record_num` = '"+num+"' WHERE (`id` = '1')";
        statement.execute(sql);
    }
    public void close() {
        try {
            if (preparedStatement!=null)
                preparedStatement.close();
            if (statement!=null)
                statement.close();
            connection.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
