package dataintegration;


import UTILS.XLSHelper;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by liqiyang on 2018/3/9.
 *
 * 思路：
 *  1. 获取excel中的表名和字段名
 *  2. 将表名和字段名格式化到脚本中
 **/
public class pdt {
     static String table_title = "表名" ;
     static String col_title = "字段名";
     static String null_string ="空";
     static String space_string="";

     static String ods_table_name ="";
     static String ods_col_name="";

    public static void main(String[] args) throws Exception{
        String filePathxlsx = "/Users/dengyuhao/Desktop/test.xlsx";
        Object[][] sheet = XLSHelper.readSheet(filePathxlsx,"工作表1");
        List tableList = getTables(sheet);
        printDataxScript(tableList);

    }


    private static List getTables(Object[][] sheet) throws Exception {
        String flag = null;
        List tables = new ArrayList();
        List table = new ArrayList();
        List columns = new ArrayList();
        for (Object[] row : sheet) {
            System.out.println(row.length);
//            遍历sheet页的excel 行
            if (table_title.equals(row[0])) {     // 识别表头 "表名"
                if (col_title.equals(flag)|| null_string.equals(flag)) {
                    table.add(columns);
                    tables.add(table);
                    table = new ArrayList();
                    columns = new ArrayList();
                }
//                flag赋值为 table_title "表名"
                flag = table_title;
                continue;
            } else if (col_title.equals(row[0])) { // 识别字段标题"字段名"
                /*将flag赋值为col_title ，为后续添加字段名做准备*/
                flag = col_title;
                continue;
            } else if (null == row[0] || space_string.equals(row[0])) { // 匹配到空行或者""字符行
                flag = null_string;
                continue;
            }
//            正常添加表名
            if (table_title.equals(flag)) {
//            将表的英文名添加到table 这个list中
                table.add(row[0]);
//                table.add(row[1]);
            } else if (col_title.equals(flag)) {
                List column = new ArrayList();
                column.add(row[0]);  //字段名
//                column.add(row[1]);     //字段类型
//                String c3 = (String) row[2] != null ? (String) row[2]  : "";
//                column.add(c3.replace("\n","").replace("\r",""));     //字段注释
                columns.add(column);
            }
        }
        System.out.println(sheet.length);
        table.add(columns);
        tables.add(table);
        return tables;
    }




    public static String printDataxScript(List<List> tables){
        for(List table:tables){
//            System.out.println("table contains: " + table);
            ods_table_name = table.get(0).toString();
//            System.out.println("table_name is : " + ods_table_name);
            ods_col_name="";
            /*获取字段列表*/
            List<List> columns = (List) table.get(1);
            for( int i =0 ; i< columns.size() - 1;i++){
                ods_col_name += "\r\n\t\t\t\"" + columns.get(i).get(0).toString() + "\"," ;
            }
//            System.out.println("ods_col_name is : " + ods_col_name);
//            System.out.println(columns.size());
//            List lens = getLens(columns);
//            System.out.println(lens);
            String sql = CONTENT.replace("&&&sourceColumns",ods_col_name.substring(1,ods_col_name.length()-1))
                    .replace("&&&sourceTableName",ods_table_name)
                    .replace("&&&targetColumns",ods_col_name.substring(1,ods_col_name.length()-1))
                    .replace("&&&targetTableName",ods_table_name)
//                    .replace("&&&deleteTableName",tableName)
//                    .replace("&&&deleteCondition",deleteCondition)
                    ;
            System.out.println(sql);
            System.out.println("-----------------------------------------------");
        }
        return "";
    }



    private static final String CONTENT = "" +
            "{\n" +
            "  \"type\": \"job\",\n" +
            "  \"version\": \"1.0\",\n" +
            "  \"configuration\": {\n" +
            "    \"setting\": {\n" +
            "      \"errorLimit\": {\n" +
            "        \"record\": \"0\"\n" +
            "      },\n" +
            "      \"speed\": {\n" +
            "        \"mbps\": \"1\",\n" +
            "        \"concurrent\": 1,\n" +
            "        \"dmu\": 1,\n" +
            "        \"throttle\": false\n" +
            "      }\n" +
            "    },\n" +
            "    \"reader\": {\n" +
            "      \"plugin\": \"odps\",\n" +
            "      \"parameter\": {\n" +
            "        \"datasource\": \"odps_first\",\n" +
            "        \"table\": \"&&&sourceTableName\",\n" +
            "        \"column\": [ &&&sourceColumns " +
            "\n" +
            "            ],\n" +
            "        \"partition\": \"ds=${bdp.system.bizdate}\"\n" +
            "      }\n" +
            "    },\n" +
            "    \"writer\": {\n" +
            "      \"plugin\": \"oracle\",\n" +
            "      \"parameter\": {  \n" +
            "        \"datasource\": \"toc_test\",\n" +
            "        \"table\": \"&&&targetTableName\",\n" +
            "        \"column\": [ &&&targetColumns " +
            "           \n" +
            "            ],\n" +
            "        \"preSql\": [],\n" +
            "        \"postSql\": []\n" +
            "      }\n" +
            "    }\n" +
            "  }\n" +
            "}"
            ;
}
