package org.example.dao;

import com.csvreader.CsvReader;
import org.example.ulit.MyUlit;

import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.*;

import java.util.List;

public class Dao {
    String sql;
    MyUlit myUlit;
    ResultSet resultSet;
    Statement statement = null;
    PreparedStatement preparedStatement = null;
    Connection connection;
    CsvReader csvReader;
    String schemas;
    String recordTableName;

    public void setRecordTableName(String recordTableName) {
        this.recordTableName = recordTableName;
    }

    public void setSchemes(String schemas) {
        this.schemas = schemas;
    }
    public void resetCsvReader(CsvReader csvReader) {
        this.csvReader = csvReader;
    }

    public Dao(MyUlit m){
        connection = new Conn().getCon();
        this.myUlit=m;
    }
    //搭建必要数据表结构
    public void mysqlInitToTableRule() throws SQLException {
        String sql1="CREATE SCHEMA IF NOT EXISTS`new_schema` DEFAULT CHARACTER SET utf8 ";
        String sql2="CREATE TABLE IF NOT EXISTS `"+schemas+"`.`"+recordTableName+"` (" +
                "  `id` INT NOT NULL AUTO_INCREMENT," +
                "  `table_name` VARCHAR(255) NULL," +
                "  `record_count` INT," +
                "  `chinese_table_name` VARCHAR(255) NULL," +
                "  `create_time` DATETIME NULL," +
                "  `remark` VARCHAR(255) NULL," +
                "  PRIMARY KEY (`id`));";
        statement = connection.createStatement();
        statement.execute(sql1);
        statement.execute(sql2);
        statement.close();
        statement=null;
    }
    public void initTempTable(String [] colName,int tempForCsvLone) throws SQLException {
        drop_table(schemas,"tempForCsv");
        creat_table(myUlit.getCreat_tableSql(this.schemas,colName,tempForCsvLone));

    }

    public void insertTempTable(String tableName,int tempForCsvLine,int tempForCsvLong) throws SQLException, IOException {
        String sql1;
        int colNum ;//列计数器，防止有字符串中出现,导致的分表错误。
        String[] colName;
        String sql;
        connection.setAutoCommit(false);
        CsvReader csvReader = myUlit.getNewCsvreader();

        //读表头
        csvReader.readHeaders();
        colName = csvReader.getHeaders();

        sql= myUlit.getInsert_tableSql(this.schemas,tableName,colName);//获得一个insert的开头,?,?,?,?,?的预处理格式

        if (statement==null)
            statement=connection.createStatement();
        if (preparedStatement==null)
            preparedStatement = connection.prepareStatement(sql);

        csvReader.setSafetySwitch(false);

        for (int i=0;i<tempForCsvLine;i++)
        {   colNum=1;
            csvReader.readRecord();
            for (String b:csvReader.getValues()){

                if (b.length()>tempForCsvLong)
                    b=b.substring(0,tempForCsvLong);
                if (csvReader.getHeaderCount()<colNum)
                    break;
                preparedStatement.setString(colNum,b);
                colNum++;
            }
            preparedStatement.addBatch();
        }
            preparedStatement.executeBatch();
            connection.commit();

        csvReader.close();
        connection.setAutoCommit(true);
    }

    public void insertCsv(String csvTableName,int BatchLong,int tempForCsvLong,String chinese_table_name,String remark) throws SQLException, IOException {
        String sqlRecord="insert into `"+schemas+"`.`"+recordTableName+ "`" +
                " (`table_name`, `record_count`, `chinese_table_name`, `create_time`, `remark`) " +
                "VALUES ('"+csvTableName+"', '0', '"+chinese_table_name+"', '"+myUlit.getDate()+"', '"+remark+"');";


        statement=connection.createStatement();
        statement.execute(sqlRecord);



        csvReader=myUlit.getNewCsvreader();
        String sql1;
        connection.setAutoCommit(false);
        csvReader.readHeaders();
        String[] colName = csvReader.getHeaders();

        List<Integer> type=myUlit.getType(colName);


        String sql= myUlit.getInsert_tableSql(schemas,csvTableName,colName);

        int catch_num=0;
        long allRecordNum=0;
        int i;

        if (preparedStatement==null)
            preparedStatement = connection.prepareStatement(sql);
        csvReader.setSafetySwitch(false);

        while (csvReader.readRecord()){
            i=0;
            for (String b:csvReader.getValues()){
                if (b.length()>tempForCsvLong)
                    b="";
                if (i>colName.length)
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

            if (catch_num>BatchLong-1){
                preparedStatement.executeBatch();
                preparedStatement.clearBatch();
                sql1="UPDATE `"+schemas+"`.`"+recordTableName+"` SET `record_count` = '"+allRecordNum+"' WHERE (`table_name` = '"+csvTableName+"')";
                statement.execute(sql1);
                connection.commit();
                catch_num=0;
            }
        }
        if (catch_num!=0){

            preparedStatement.executeBatch();
            preparedStatement.clearBatch();

            sql1="UPDATE `"+schemas+"`.`"+recordTableName+"` SET `record_count` = '"+allRecordNum+"' WHERE (`table_name` = '"+csvTableName+"')";
            statement.execute(sql1);
            connection.commit();
        }

        preparedStatement.close();
        statement.close();
        csvReader.close();
//      statement.execute("insert into test_table(id,name) values (5,\'胡i()（）\')");
    }
    public void creat_table(String sql1) throws SQLException {
        statement = connection.createStatement();
        statement.execute(sql1);
        statement.close();
    }

    public void drop_table(String schemas,String tablename) throws SQLException {
        sql="Drop table if exists `"+schemas+"`.`"+tablename+"`";
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
            if (resultSet!=null)
                resultSet.close();
            if (preparedStatement!=null)
                preparedStatement.close();
            if (statement!=null)
                statement.close();
            if (csvReader!=null)
                csvReader.close();
            connection.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
