import com.csvreader.CsvReader;
import org.example.dao.Dao;
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

    @Before
    public void before(){
    dao=new Dao();
    myUlit=new MyUlit();
    }

    @Test
    public void testMyUlit_lookCsv() throws IOException {
        myUlit.lookCsv(100,"E:/cfmx.csv",',');
    }
    @Test
    public void testMyUlit_getCsvHead() throws IOException {
        String a[]= myUlit.getCsvHead("E:/cfmx.csv",',');
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
        String colName[]= myUlit.getCsvHead("E:/jzmx.csv",',');
        String a=myUlit.getCreat_tableSql("work","jzmx",colName);
        dao.creat_table(a);
    }
    @Test
    public void Dao_DropTable () throws SQLException {
        dao.drop_table("test2");
    }

    @Test
    public void Dao_truncate_table() throws SQLException {
        dao.truncate_table("`work`.`jzmx`;");
    }
    @Test
    public void getInsert_tableSql() throws IOException {
        String colName[]= myUlit.getCsvHead("E:/jzmx.csv",',');
        String a= myUlit.getInsert_tableSql("work","jzmx",colName);
        System.out.println(a);
    }


    @Test
    public void insert() throws SQLException, IOException {
        dao.insertJzmx();
        dao.close();
        System.out.println("导入完成");
    }

    @Test
    public void insert1() throws SQLException, IOException {
        dao.insertCfmx();
        dao.close();
        System.out.println("导入完成");
    }
    @Test
    public void a() throws IOException {
        String colName[]= myUlit.getCsvHead("E:/jzmx.csv",',');
        List list=myUlit.getType(colName);
        System.out.println(list.toString());
    }
    @Test
    public void b(){
        String b="2016/25/3 00.2123523dfg";
        if(b.matches("\\d{4}\\/\\d{1,2}\\/\\d{1,2}.*"))
            System.out.println("true");
    }
    @After
    public void after(){
        dao.close();
    }

}
