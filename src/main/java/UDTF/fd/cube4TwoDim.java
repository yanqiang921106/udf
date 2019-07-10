package UDTF.fd;

import com.aliyun.odps.udf.ExecutionContext;
import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.UDTF;
import com.aliyun.odps.udf.annotation.Resolve;

import java.util.ArrayList;

// TODO define input and output types, e.g. "string,string->string,bigint".
//input_param: 'ALL',2,100,tb12,3C,CN
// 此处resolve的数据类型为odps上的
//@Resolve({"string,bigint,string,string,string,string->string,string,string,double"})
@Resolve({"string,string,string,string->string,string,string"})
public class cube4TwoDim extends UDTF {
    private static final int constParam =2;

    @Override
    public void setup(ExecutionContext ctx) throws UDFException {

    }

    @Override
    public void process(Object[] args) throws UDFException {
        // TODO
        final  String splitStr=":::";

        //参数总的个数
        int param_len = args.length;

        // 传入的参数中,表中的字段个数
        int tbl_cnt = param_len - 1;

        String[] index = null;

        /*维度列表*/
        ArrayList<String> dim_list = new ArrayList<String>();

        /*组合维度*/
        ArrayList<String> combination = new ArrayList<String>();

        String dim_first = null;

        /*空值作-911处理*/
        for(int i=0 ;i<args.length;i++){
            if(args[i] == null){
                args[i] =new String("-911");
            }
        }

        /*传入参数中的默认值*/
        String default_str =args[0].toString() ;

        /*异常情况*/
        if(default_str.length() == 0){
            throw new UDFException("default param is valid!");
        }

        int index_cnt = 1 ;

        /*传入的参数中的维度的个数*/
        int dim_cnt = tbl_cnt - index_cnt ;


        /*指标数组*/
        index = new String[index_cnt];


        /*维度列表初始化*/
        for(int i=0;i<dim_cnt;i++){
            dim_list.add(args[param_len-1-i].toString().concat(splitStr).concat(default_str));
        }

         /*指标数组初始化*/
        for(int i=0 ;i<index_cnt;i++){
            if(args[param_len-1-dim_cnt-i] == null){
                args[param_len-1-dim_cnt-i] = 0.0f;
            }
                index[i]= args[param_len-1-dim_cnt-i].toString();
        }


        if(!dim_list.isEmpty() && dim_list.size()>=2){
            dim_first = dim_list.get(0);
            dim_list.remove(0);
            combination =  cycle(dim_first,dim_list,splitStr);
        }else if(dim_list.size() == 1){
            for (String res1 : dim_list.get(0).split(splitStr)){
                combination.add(res1);
            }
        }

        String[] out = null;
        System.out.println("维度组合后的结果为:");
        if(index.length>0){
            for(String res:combination) {
                for (int i = 0; i < index.length; i++) {
                    res = res.concat(splitStr).concat(index[i]);
                }
                out = res.split(splitStr);
                //        测试打印数据,每一次forward都会将数据输出到目标集合中.
                forward(out[0],out[1],out[2]);
//                forward(out);
            }
        }else {
            for(String res:combination) {
                out = res.split(":::");
                //        测试打印数据,每一次forward都会将数据输出到目标集合中.
                forward(out[0],out[1],out[2]);
//                forward(out);
            }
        }

    }



    @Override
    public void close() throws UDFException {

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