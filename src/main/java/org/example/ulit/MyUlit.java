package org.example.ulit;

import com.csvreader.CsvReader;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class MyUlit {
    CsvReader csvReader;
    DateTimeFormatter dateTimeFormatter;
    String csvPath;
    char splitChar;
    String csvEnCode;



    public MyUlit(String csvPath, char splitChar,String csvEnCode) {
        this.csvPath = csvPath;
        this.splitChar = splitChar;
        this.csvEnCode=csvEnCode;
    }

    public CsvReader getNewCsvreader() throws FileNotFoundException {
        return new CsvReader(csvPath,splitChar,Charset.forName(csvEnCode));
    }
    /*
                   得到一个时间戳，用于
                * */
    public Timestamp getTextTimestap(String text) {
        dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy/M/d HH:mm:ss");
        if (text!="")
        {

            text=text.split("\\.")[0];
//            System.out.println(text);
        }
        else
            text=null;
        return Timestamp.valueOf(LocalDateTime.parse(text, dateTimeFormatter));
    }
    public void lookCsv(int lineNum, String path, char split) throws IOException {
        String[] record;
        csvReader = getNewCsvreader();
        for (int i = 0; i < lineNum; i++) {
            int n=0;
            if (csvReader.readRecord()) {
                record = csvReader.getValues();
                System.out.print("总长度：" + record.length + "----");
                for (String a : record) {
                    System.out.print("("+n+"/" + a + ")");
                    n++;
                }
                System.out.print("\n");
            } else {
                csvReader.close();
                break;
            }
        }
        csvReader.close();
    }
    public String[]  getCsvHead() throws IOException {
        csvReader = getNewCsvreader();
        csvReader.readHeaders();
        String [] a=csvReader.getHeaders();
        return a;
    }
    public String getCreat_tableSql(String dataBaseNane,String tableName,String [] colName){
        int coltype;
        String sql="create table if not exists"+" `"+dataBaseNane+"`."+"`"+tableName+"`(";
        sql+="`自设编号` int unsigned auto_increment,";
        for (String a:colName)
        {   coltype=0;
            if (a.matches(".*费$"))
                coltype=1;
            if (a.matches(".*金额$"))
                coltype=1;
            if (a.matches(".*支付$"))
                coltype=1;
            if (a.matches(".*救助$"))
                coltype=1;
            if (a.matches(".*数量$"))
                coltype=1;
            if (a.matches(".*单价$"))
                coltype=1;
            if (a.matches(".*总额$"))
                coltype=1;
            if (a.matches(".*补助\\(市级\\)$"))
                coltype=1;
            if (a.matches(".*日期$"))
                coltype=2;
            if (a.matches(".*时间$"))
                coltype=2;

            switch (coltype){
                case 1:sql+="`"+a+"` float,";break;
                case 2:sql+="`"+a+"` datetime,";break;
                default: sql+="`"+a+"` varchar(255),";
            }
        }
        sql+="primary key(`自设编号`)";
        sql+=")engine=InnoDB DEFAULT CHARACTER SET=utf8;";

        return sql;
    }
    public String getCreat_tableSql(String schemas,String [] colName,int tempForCsvLone){
        String sql="create table if not exists"+" `"+schemas+"`.`tempForCsv`(";
        for (String a:colName){
            sql+="`"+a+"` varchar("+tempForCsvLone+"),";
        }
        sql=sql.substring(0,sql.length()-1);
        sql+=")engine=InnoDB DEFAULT CHARACTER SET=utf8;";
        return sql;
    }

    public List<Integer> getType(String [] colName){
        //0:字符 1：float  2：date
        List<Integer> typeArray=new ArrayList();
        for (String a:colName){

            if (a.matches(".*费$")){
                typeArray.add(1);
                continue;
            }
            if (a.matches(".*金额$")) {
                typeArray.add(1);
                continue;
            }
            if (a.matches(".*支付$")){
                typeArray.add(1);
                continue;
            }
            if (a.matches(".*救助$"))
            {
                typeArray.add(1);
                continue;
            }
            if (a.matches(".*数量$"))
            {
                typeArray.add(1);
                continue;
            }
            if (a.matches(".*单价$"))
            {
                typeArray.add(1);
                continue;
            }
            if (a.matches(".*总额$"))
            {
                typeArray.add(1);
                continue;
            }
            if (a.matches(".*补助\\(市级\\)$"))
            {
                typeArray.add(1);
                continue;
            }
            if (a.matches(".*日期$"))
            {
                typeArray.add(2);
                continue;
            }
            if (a.matches(".*时间$"))
            {
                typeArray.add(2);
                continue;
            }
            typeArray.add(0);

        }

        return typeArray;
    }
    public String getInsert_tableSql(String dataBaseNane,String tableName,String [] colName){
        String sqlB=" values (";
        String sql="insert into `"+dataBaseNane+"`."+"`"+tableName+"` (";
        for (String a:colName)
        {
            sql+="`"+a+"`,";
            sqlB+="?,";
        }
        sql=sql.substring(0,sql.length()-1);
        sqlB=sqlB.substring(0,sqlB.length()-1);
        sql+=")";
        sqlB+=")";

        return sql+sqlB;
    }
    public String getDate(){
        return LocalDateTime.now().toString();
    }
}
