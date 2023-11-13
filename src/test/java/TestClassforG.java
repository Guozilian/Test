import com.csvreader.CsvReader;
import org.example.dao.Dao;
import org.example.service.InsertCSVtoMysql;
import org.example.ulit.MyUlit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.security.PublicKey;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class TestClassforG {

    Dao dao;
    MyUlit myUlit;
    String csvPath="E:/cfmx.csv";
    InsertCSVtoMysql insertCSVtoMysql;
    char splitChar=',';

    @Before
    public void before() throws SQLException {

    myUlit=new MyUlit(csvPath,splitChar,"utf-8");


    insertCSVtoMysql=new InsertCSVtoMysql();
    dao=insertCSVtoMysql.dao;
    dao.setSchemes("work");

    }

    @Test
    public void testMyUlit_lookCsv() throws IOException {
        myUlit.lookCsv(100,"E:/cfmx.csv",',');
    }
    @Test
    public void testMyUlit_getCsvHead() throws IOException {
        String a[]= myUlit.getCsvHead();
        for (String b:a){
            System.out.printf(b+",");
        }
    }
    @Test
    public void testMyUlit_getCreat_tableSql(){
        String [] colName={"生日期","生活费"};
        String a=myUlit.getCreat_tableSql("work","test2",colName);
        System.out.println(a);
    }

    @Test
    public void Dao_creatTable() throws SQLException, IOException {
        String colName[]= myUlit.getCsvHead();
        String a=myUlit.getCreat_tableSql("work","jzmx",colName);
        dao.creat_table(a);
    }
    @Test
    public void Dao_DropTable () throws SQLException {
        dao.drop_table("work","test2");
    }

    @Test
    public void Dao_truncate_table() throws SQLException {
        dao.truncate_table("`work`.`jzmx`;");
    }
    @Test
    public void getInsert_tableSql() throws IOException {
        String colName[]= myUlit.getCsvHead();
        String a= myUlit.getInsert_tableSql("work","jzmx",colName);
        System.out.println(a);
    }
    @Test
    public void a() throws IOException {
        String colName[]= myUlit.getCsvHead();
        for (String a:colName)
            System.out.print(a+"$#$");
            System.out.print("\n");
        List list=myUlit.getType(colName);
        System.out.println(list.toString());
    }
    @Test
    public void b() throws SQLException {
//        String b="2016/25/3 00.2123523dfg";
//        if(b.matches("\\d{4}\\/\\d{1,2}\\/\\d{1,2}.*"))
//            System.out.println("true");
    }
    @Test
    public void InsertCsv1() throws SQLException, IOException {
        insertCSVtoMysql.loadingCsvToMysql();
    }
    @Test
    public void InsertCsv2() throws SQLException, IOException {
        insertCSVtoMysql.insertCsv();
    }
    @Test
    public void cc(){
        System.out.printf(myUlit.getDate());
    }
    @After
    public void after(){
        dao.close();
        insertCSVtoMysql.close();
    }

}
