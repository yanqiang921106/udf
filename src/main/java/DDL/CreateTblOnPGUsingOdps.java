package DDL;


import UTILS.XLSHelper;
import UTILS.myUtils;

import java.io.File;
import java.io.FileWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by liqiyang on 2018/3/2.
 **/
public class CreateTblOnPGUsingOdps {

//    private static final String DATABASE_NAME = "mf";

    public static void main(String[] args) throws Exception {


        /*
                    汇总层建表语句
         */
//        String filePathxlsx = "E:\\test1.xlsx";
//        String filePathxlsx = "E:\\1.xls";
        String filePathxlsx = "/Users/yanqiang/Downloads/信息中心ads_mysql.xlsx";
//        String filePathxlsx = "E:\\项目管理\\快鱼数据中台\\03项目开发\\615二期\\拓展门店主题-ads.xlsx";
//        String filePathxlsx = "E:\\项目管理\\快鱼数据中台\\维度建模（开发）\\DWS\\库存DWS层设计.xlsx";
//          String filePathxlsx = "C:\\Users\\Administrator\\Desktop\\销存分析页面ADS4周倜.xlsx";
//        String filePathxlsx = "C:\\Users\\Administrator\\Desktop\\标准词4毕彩萍.xlsx";
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
        String tableComment = "" ;
        String tableName = "";
        List tables = new ArrayList();
        List table = new ArrayList();
        List columns = new ArrayList();
        for (Object[] row : sheet) {
            if ("表名".equals(row[0])) {
                if ("字段".equals(flg)||"空".equals(flg)) {
                    table.add(columns);
                    tables.add(table);
                    table = new ArrayList();
                    columns = new ArrayList();
                }
                flg = "表名";
                continue;
            } else if ("字段".equals(row[0])) {
                flg = "字段";
                continue;
            } else if (null == row[0] || "".equals(row[0])) {
                flg = "空";
                continue;
            }

            if ("表名".equals(flg)) {
                table.add(row[0]);
                table.add(row[1]);
            } else if ("字段".equals(flg)) {
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
                    "\n" + "CREATE TABLE IF NOT EXISTS "+table.get(0) + " (\n";
            for (int i = 0;i<columns.size();i++){
                if (i == columns.size()-2){     // 表最后一行
                    createSql+= String.format("%-"+ String.valueOf((Integer) lens.get(0)+4)+"s",columns.get(i).get(0))
                            + String.format("%-"+ String.valueOf((Integer) lens.get(1)+4)+"s",myUtils.replaceOdps2PG(columns.get(i).get(1).toString())) +"\n";
                }else if(i < columns.size()-2){     // 非最后一行
                    createSql+= String.format("%-"+ String.valueOf((Integer)lens.get(0)+4)+"s",columns.get(i).get(0))
                            + String.format("%-"+ String.valueOf((Integer)lens.get(1)+4)+"s",myUtils.replaceOdps2PG(columns.get(i).get(1).toString())) + ",\n";
                }
            }
            createSql+=");\n\nCOMMENT ON  TABLE "+table.get(0) + " IS '" + table.get(1)+"';\n";

            for (int i = 0;i<columns.size()-1;i++){
                createSql+= "COMMENT ON COLUMN "+table.get(0)+"."+ columns.get(i).get(0) + " IS '" + columns.get(i).get(2)+"';\n";

            }

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
