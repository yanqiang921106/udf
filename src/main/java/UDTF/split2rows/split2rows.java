package UDTF.split2rows;

import com.aliyun.odps.udf.ExecutionContext;
import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.UDTF;
import com.aliyun.odps.udf.annotation.Resolve;

// TODO define input and output types, e.g. "string,string->string,bigint".
@Resolve({"string,string,string->string,string"})
public class split2rows extends UDTF {

//    private Object[] args;

    @Override
    public void setup(ExecutionContext ctx) throws UDFException {

    }

    @Override
    public void process(Object[] args) throws UDFException {

        String copyIndex = args[0].toString();

        String ToBeCuttedIndex = args[1].toString();

        String regex = args[2].toString();

        for(String t: ToBeCuttedIndex.split(regex)){
            forward(copyIndex,t);
        }
    }



    @Override
    public void close() throws UDFException {

    }

}