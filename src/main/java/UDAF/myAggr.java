package UDAF;

import com.aliyun.odps.io.DoubleWritable;
import com.aliyun.odps.io.Writable;
import com.aliyun.odps.udf.ExecutionContext;
import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.Aggregator;
import com.aliyun.odps.udf.annotation.Resolve;

import java.util.HashSet;

// TODO define input and output types, e.g. "double->double".
//@Resolve({"integer->integer"})
@Resolve({"string,string,string,string,string,double->string,string,double,double"})
public class myAggr extends Aggregator {

    private DoubleWritable res = new DoubleWritable();

    private HashSet<String> index1 = new HashSet<String>();

    private HashSet<String> index2 = new HashSet<String>();

    @Override
    public void setup(ExecutionContext ctx) throws UDFException {

    }

    @Override
    public Writable newBuffer() {
        // TODO
       return new DoubleWritable();
    }

    @Override
    public void iterate(Writable buffer, Writable[] args) throws UDFException {
        // TODO
        System.out.println("the length of the args is :" + args.length);
        for (Writable a : args){
            System.out.println("the type of a is :"+a.getClass());
            System.out.println("the element of the args is:" + a );
        }

        index1.add(args[0].toString());
        index2.add(args[1].toString());


    }

    @Override
    public void merge(Writable buffer, Writable partial) throws UDFException {
        // TODO
    }

    @Override
    public Writable terminate(Writable buffer) throws UDFException {
        // TODO
        res.set(8.88);
        MyIterator(index1);
        MyIterator(index2);
        return res;

    }

    @Override
    public void close() throws UDFException {

    }


//    遍历set元素
    public void MyIterator(HashSet<String> mySet){
         for (String index : mySet){
             System.out.println(index);
         }
    }

}