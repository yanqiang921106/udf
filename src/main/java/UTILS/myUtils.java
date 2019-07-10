package UTILS;

import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.UDTF;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by Administrator on 2018\6\15 0015.
 */
public class myUtils{
    public static String replaceOdps2PG(String args){
        if(args == null || args.length() <=0 ){
            System.err.println("字段类型有误！！！");
            return null;
        }else{
            String str = args.trim().toLowerCase() ;
            if(str.equals("double")){
                return new String("decimal");
            }

            if(str.equals("string")){
                return new String("varchar");
            }

            if(str.equals("datetime")){
                return new String("date");
            }

            return str;

        }
    }


    /*
* 设置循环的递归函数
* */
    public ArrayList<String> cycle(String first,ArrayList<String> args,String splitStr){
        ArrayList<String> last_res = new ArrayList<String>();
        if (args.isEmpty()){
            System.out.println("ERROR");
            return null;
        }

        if(args.size() == 1 ){
            String[] firstStr=first.split(splitStr);
            String[] secondStr=args.get(0).split(splitStr);
            for(int i=0;i<firstStr.length;i++){
                for(int j=0;j<secondStr.length;j++){
                    last_res.add(firstStr[i]+splitStr+secondStr[j]);
                }
            }
            return last_res;
        }

        // 如果args的长度>1 ,则继续传递
        String firsttmp=args.get(0);
        args.remove(0);

        ArrayList<String> tmpArrayList = cycle(firsttmp,args,splitStr);

        String[] firstArray = first.split(splitStr);

        for(int i=0;i<firstArray.length;i++) {
            for(int j=0;j<tmpArrayList.size();j++){
                last_res.add(firstArray[i].concat(splitStr).concat(tmpArrayList.get(j)));

            }

        }

        return last_res;

    }

}
