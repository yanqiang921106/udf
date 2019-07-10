package UDF;

import com.aliyun.odps.udf.UDF;
import org.apache.spark.sql.execution.columnar.LONG;

public class helloUdf extends UDF {
    // TODO define parameters and return type, e.g:  public String evaluate(String a, String b)
    public String evaluate(String s) {
        return "hello :" + s;
    }


    public String evaluate(Long s) {
        return "hello :" + s.toString();
    }
}