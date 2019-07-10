package dataintegration;


import UTILS.XLSHelper;

/**
 * Created by liqiyang on 2018/3/9.
 **/
public class CreateDataXJobFromExcel {
    private final static String defaultFS = "hdfs://172.16.4.5:8020/";
    private final static String jdbcUrl = "jdbc:mysql://rm-bp17loanys4ci7ml0ao.mysql.rds.aliyuncs.com:3306/mf?characterEncoding=UTF-8";
    private final static String userName = "bitmind";
    private final static String password = "Hello1234";
    private final static String filePath = "/Users/dengyuhao/Desktop/无人店数据字典全（截止二期）.xlsx";


    public static void main(String[] args) throws Exception{
        Object[][] sheet = XLSHelper.readSheet(filePath,"用户分析");
        create(sheet);

    }

    public static void create(Object[][] sheet){
        int index = 1;
        String sourceColumns = "", targetColumns = "", createSql = null;
        String tableName="",deleteCondition="";
        int i = 0;
        for (Object[] row : sheet) {
            if(null == row[0] || "表名".equals(row[0])) continue;
            if(tableName.equals(row[0])){
                targetColumns += "\r\n\t\t\t\t\t\t\t\"" + row[2] + "\"," ;
                sourceColumns += "\r\n\t\t\t\t\t\t\t{ \"index\" : "  + (i++) + ", \"type\" : \"" + getType((String)row[3]) + "\" },";
            }else { // 下一张表
                // 先将之前的表创建
                deleteCondition = tableName.endsWith("m")? "substring('${bdp.system.bizdate}',1,6)":"'${bdp.system.bizdate}'";
                createSql = "".equals(sourceColumns) || "".equals(targetColumns) ? null
                        : CONTENT.replace("&&&sourceColumns",sourceColumns.substring(1,sourceColumns.length()-1))
                        .replace("&&&sourceTableName",tableName)
                        .replace("&&&targetColumns",targetColumns.substring(1,targetColumns.length()-1))
                        .replace("&&&targetTableName",tableName)
                        .replace("&&&deleteTableName",tableName)
                        .replace("&&&deleteCondition",deleteCondition)
                        .replace("&&&jdbcUrl",jdbcUrl)
                        .replace("&&&defaultFS",defaultFS)
                        .replace("&&&userName",userName)
                        .replace("&&&password",password);
                if(null!=createSql){
                    System.out.println("\n-- " + (index++) + " -------------------------------- datax_" + tableName);
                    System.out.println(createSql); // 创建表
                    i = 0;
                }

                //开始记录新的表

                tableName = (String) row[0];
                targetColumns = "\r\n\t\t\t\t\t\t\t\"" + row[2] + "\"," ;
                sourceColumns = "\r\n\t\t\t\t\t\t\t{ \"index\" : "  + (i++) + ", \"type\" : \"" + getType((String)row[3]) + "\" },";
            }
        }
        createSql = "".equals(sourceColumns) || "".equals(targetColumns) ? null
                : CONTENT.replace("&&&sourceColumns",sourceColumns.substring(1,sourceColumns.length()-1))
                .replace("&&&sourceTableName",tableName)
                .replace("&&&targetColumns",targetColumns.substring(1,targetColumns.length()-1))
                .replace("&&&targetTableName",tableName)
                .replace("&&&deleteTableName",tableName)
                .replace("&&&deleteCondition",deleteCondition)
                .replace("&&&jdbcUrl",jdbcUrl)
                .replace("&&&defaultFS",defaultFS)
                .replace("&&&userName",userName)
                .replace("&&&password",password);
        if(null!=createSql){
            System.out.println("\n-- " + (index++) + " -------------------------------- " + tableName);
            System.out.println(createSql); // 创建表
        }




    }

    private static String getType(String type){
        int index = type.indexOf("(");
        if( index > 0 ) type = type.substring(0,index);
        switch (type.toLowerCase()){
            case "int": case "bigint": case "tinyint":
                return "LONG";
            case "varchar": case "string":case "datetime":
                return "STRING";
            case "double": case "decimal":
                return "DOUBLE";
            default:
                return "ERROR";
        }
    }


    private static final String CONTENT = "{ \"setting\": {},\n" +
            "    \"job\": {\n" +
            "        \"setting\": { \n" +
            "\t\t\t\"speed\": { \n" +
            "\t\t\t\"channel\": 1\n" +
            "         \t},\n" +
            "         \t\"errorLimit\": {\n" +
            "                \"record\": 0,\n" +
            "                \"percentage\": 0.02\n" +
            "            }\n" +
            "        },\n" +
            "        \"content\": [{\n" +
            "                \"reader\": {\n" +
            "                    \"name\": \"hdfsreader\",\n" +
            "                    \"parameter\": {\n" +
            "                        \"path\": \"/apps/hive/warehouse/unmannedshop.db/&&&sourceTableName/ds=${bdp.system.bizdate}\",\n" +
            "                        \"defaultFS\": \"&&&defaultFS\",\n" +
            "                        \"column\": [ &&&sourceColumns ],\n" +
            "                        \"fileType\": \"text\",\n" +
            "                        \"encoding\": \"UTF-8\",\n" +
            "                        \"fieldDelimiter\": \"\\t\",\n" +
            "\t\t                \"nullFormat\":\"\\\\N\"\n" +
            "                    }\n" +
            "                },\n" +
            "                \"writer\": {\n" +
            "                    \"name\": \"mysqlwriter\",\n" +
            "                    \"parameter\": {\n" +
            "                        \"writeMode\": \"insert\",\n" +
            "                        \"username\": \"&&&userName\",\n" +
            "                        \"password\": \"&&&password\",\n" +
            "                        \"column\": [ &&&targetColumns ],\n" +
            "                        \"preSql\": [ \"delete from &&&deleteTableName where dt_date = &&&deleteCondition\" ],\n" +
            "                        \"postSql\": [ \"\" ],\n" +
            "                        \"connection\": [ {\n" +
            "                                \"jdbcUrl\": \"&&&jdbcUrl\",\n" +
            "                                \"table\": [ \"&&&targetTableName\" ]\n" +
            "                            }\n" +
            "                        ]\n" +
            "                    }\n" +
            "                }\n" +
            "            }\n" +
            "        ]\n" +
            "    }\n" +
            "}";

}
