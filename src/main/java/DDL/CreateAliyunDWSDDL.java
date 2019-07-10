package DDL;


import UTILS.XLSHelper;

import java.io.File;
import java.io.FileWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by liqiyang on 2018/3/2.
 **/
public class CreateAliyunDWSDDL {

//    private static final String DATABASE_NAME = "mf";

    public static void main(String[] args) throws Exception {


        /*
                    汇总层建表语句
         */
         String filePathxlsx = "/Users/dengyuhao/Desktop/test.xlsx";
//        String filePathxlsx = "E:\\项目管理\\快鱼数据中台\\维度建模（开发）\\DWS\\库存DWS层设计.xlsx";
//        String filePathxlsx = "E:\\项目管理\\快鱼数据中台\\03项目开发\\615二期\\拓展门店主题-cdm.xlsx";
//        String filePathxlsx = "E:\\项目管理\\快鱼数据中台\\SVN\\03 项目开发\\05 数据开发\\04 ADS\\ADS层表结构设计_库存.xlsx";
//        String filePathxlsx = "C:\\Users\\Administrator\\Desktop\\ADS层表结构设计-所有店分析(2).xlsx";
//        Object[][] sheet = XLSHelper.readSheet(filePathxlsx,1);
        Object[][] sheet = XLSHelper.readSheet(filePathxlsx,0);
        List tables = CreateTables(sheet);
        printSql(tables);

    }



    /**
     *以如下格式创建sheet中的表：
     *      a表名                         a表注释
     字段名	                    字段类型	                字段注释
     字段1	                    bigint	                日期（精确到小时）
     字段n	                    string	                注释

     b表名	                    b表注释
     字段名	                    字段类型	                字段注释
     字段1	                    bigint	                日期（精确到小时）
     字段n	                    string	                注释

     * @param sheet excel表的某个sheet
     * @throws Exception 异常
     */
    private static List CreateTables(Object[][] sheet) throws Exception {
        String names = "";
        String flg = null;
        String sql = null,createSql=null;
        String tablecomment = "" ;
        String tableName = "";
        List tables = new ArrayList();
        List table = new ArrayList();
        List columns = new ArrayList();
        for (Object[] row : sheet) {
            if ("表名".equals(row[0])) {
                if ("字段名".equals(flg)||"空".equals(flg)) {
                    table.add(columns);
                    tables.add(table);
                    table = new ArrayList();
                    columns = new ArrayList();
                }
                flg = "表名";
                continue;
            } else if ("字段名".equals(row[0])) {
                flg = "字段名";
                continue;
            } else if (null == row[0] || "".equals(row[0])) {
                flg = "空";
                continue;
            }

            if ("表名".equals(flg)) {
                table.add(row[0]);
                table.add(row[1]);
            } else if ("字段名".equals(flg)) {
                List column = new ArrayList();
                column.add(row[0]);  //字段名
                column.add(row[1]);     //字段类型
                String c3 = (String) row[2] != null ? (String) row[2]  : "";
                column.add(c3.replace("\n","").replace("\r",""));     //字段注释
                columns.add(column);
            }
        }
        table.add(columns);
        tables.add(table);
        return tables;
    }

    public static String printSql(List<List> tables){
        String dt_date = new SimpleDateFormat("yyyy.MM.dd").format(new Date());
        for(List table:tables){
            List<List> columns = (List) table.get(2);
            List lens = getLens(columns);

            String createSql = "--[Table_Name   :    "+table.get(0)+"           ]\n" +
                    "--[Author    :    风雨          ]\n" +
                    "--[Created   :    "+dt_date+"               ]\n" +
                    "--[comment   :   此处放的是"+table.get(1)+"              ]\n" +
                    "\n" + "create table if not exists "+table.get(0) + " (\n";
//            for (int i = 0;i<columns.size();i++){
//                if(i == columns.size()-1){
//                    createSql+= String.format("%-"+ String.valueOf((int)lens.get(0)+4)+"s",columns.get(i).get(0)) + String.format("%-"+ String.valueOf((int)lens.get(1)+4)+"s",columns.get(i).get(1)) +"\n";
//                }else {
//                    createSql+= String.format("%-"+ String.valueOf((int)lens.get(0)+4)+"s",columns.get(i).get(0)) + String.format("%-"+ String.valueOf((int)lens.get(1)+4)+"s",columns.get(i).get(1)) + ",\n";
//                }
//            }
//            createSql+=")\ncomment '"+table.get(1)+"'\n" +"partitioned by (ds STRING comment 'yyyymmdd');\n";
            int end = 0;
            for (int i = 0;i<columns.size();i++){
                if(i < columns.size() - 2){
//                    createSql+= String.format("%-"+ String.valueOf((int)lens.get(0)+4)+"s",columns.get(i).get(0)) + String.format("%-"+ String.valueOf((int)lens.get(1)+4)+"s",columns.get(i).get(1)) + ",\n";
                    createSql+= String.format("%-"+ String.valueOf((Integer) lens.get(0)+4)+"s",columns.get(i).get(0)) + String.format("%-"+ String.valueOf((Integer) lens.get(1)+4)+"s",columns.get(i).get(1)) + "comment '"+ columns.get(i).get(2) + "',\n";

                }else if(i == columns.size() -2 ){
//                    createSql+= String.format("%-"+ String.valueOf((int)lens.get(0)+4)+"s",columns.get(i).get(0)) + String.format("%-"+ String.valueOf((int)lens.get(1)+4)+"s",columns.get(i).get(1)) +"\n";
                    createSql+= String.format("%-"+ String.valueOf((Integer) lens.get(0)+4)+"s",columns.get(i).get(0)) + String.format("%-"+ String.valueOf((Integer)lens.get(1)+4)+"s",columns.get(i).get(1)) + "comment '"+ columns.get(i).get(2) + "'\n";
                }
                end = i ;
            }
            createSql+=")\ncomment '"+table.get(1)+"'\n" +"partitioned by ("+ String.valueOf(columns.get(end).get(0)) +" "+ String.valueOf(columns.get(end).get(1)) +" comment 'yyyymmdd')\n;";
            System.out.println("");
            System.out.println(createSql);
        }
        return "";
    }

    public static List getLens(List<List> columns){
        List<Integer> lens = new ArrayList();{{
            lens.add(0);
            lens.add(0);
        }}
        for(List<String> column :columns) {
            int columnLen = column.get(0).length();
            int typeLen = column.get(0).length();
            if (columnLen > lens.get(0)) {
                lens.set(0, columnLen);
            }
            if (typeLen > lens.get(1)) {
                lens.set(1, typeLen);
            }
        }
        return lens;
    }



    private static void writeToFile(String pathName, String fileName, String content) throws Exception {
        FileWriter fw = new FileWriter(new File(pathName + "/" + fileName));
        fw.write(content);
        fw.flush();
        fw.close();
    }



}
