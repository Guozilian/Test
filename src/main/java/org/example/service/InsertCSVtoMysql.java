package org.example.service;

import com.csvreader.CsvReader;
import org.example.dao.Dao;
import org.example.ulit.MyUlit;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.SQLException;

/*
*此类代表一个csv文件导入mysql数据库的完整周期过程
* mysql数据库构成
* mysql中有一个名为work的schemas
* 其中有两张关键表
*   record作为导入日志，用于统计导入的所有表。
*   tempForCsv为导入前的临时表，用来查看表结构，方便后续进行针对性修改。此表默认导入前100条记录。
*
* 过程如下
*   1.判断数据库是否存在work schemas，其下是否存在record和tempForCsv，若无，则新建。
*   2.按参数进行数据导入
**/
public class InsertCSVtoMysql {
    String csvPath="E:/巫溪处方明细.csv";//csv文件地址
    String tableNameForCsv="wxcfmx";//csv在数据库中表的命名（英文）
    public Dao dao;
    String csvEnCode="utf-8";//or gb2312
    MyUlit myUlit;
    String schemas="work";
    String recordTableName="record2";
    int lineLong;//每一行数据的数量
    char splitChar=',';
    int tempForCsvLine=100;//默认导入前100
    int tempForCsvLong=255;//临时表的长度默认为255
    public InsertCSVtoMysql() throws SQLException {
        System.out.println("载入设置");
        myUlit=new MyUlit(csvPath,splitChar,csvEnCode);
        dao=new Dao(myUlit);
        dao.setSchemes(schemas);
        dao.setRecordTableName(recordTableName);


        //创建schemas和对应record表
        dao.mysqlInitToTableRule();
    }

    //按要求配置相应csv文件
    public void loadingCsvToMysql() throws IOException, SQLException {
        dao.initTempTable(myUlit.getCsvHead(), tempForCsvLong);//初始化临时表
        dao.resetCsvReader(myUlit.getNewCsvreader());
        dao.insertTempTable("tempforcsv",tempForCsvLine,tempForCsvLong);//插入数据

    }
    public void insertCsv() throws SQLException, IOException {
        //创建表
        dao.creat_table(myUlit.getCreat_tableSql(schemas,tableNameForCsv, myUlit.getCsvHead()));
        //写入数据
        dao.insertCsv(tableNameForCsv,10000,tempForCsvLong,"测试医院","测试备注");
    }


    public void close(){
        dao.close();
    }

}
