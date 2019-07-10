package UDAF;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import com.aliyun.odps.io.DoubleWritable;
import com.aliyun.odps.io.Writable;
import com.aliyun.odps.udf.Aggregator;
import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.annotation.Resolve;
@Resolve({"double->double"})
public class AggrAvg extends Aggregator {


    //    AvgBuffer 类实现Writable 接口
    private static class AvgBuffer implements Writable {
        private double sum = 0;
        private long count = 0;

//        反序列化
        public void readFields(DataInput in) throws IOException {
            sum = in.readDouble();
            count = in.readLong();
            System.out.println(sum);
//            以下语句未执行
//            System.out.println("读入的文本是:"+in.readLine());
        }

//        序列化
        public void write(DataOutput out) throws IOException {
            out.writeDouble(sum);
            out.writeLong(count);

        }

    }


    private DoubleWritable ret = new DoubleWritable();

//    数据输入
    @Override
    public Writable newBuffer() {
        return new AvgBuffer();
    }

//    数据处理
    @Override
    public void iterate(Writable buffer, Writable[] args) throws UDFException {
//        DoubleWritable arg = (DoubleWritable) args[0] ;
        System.out.println("the length of the args is :" + args.length);
        for (Writable a : args){
            System.out.println("the element of the args is:" + a );
        }

        AvgBuffer buf = (AvgBuffer) buffer;
//        if (arg != null) {
//            buf.count += 1;
//            buf.sum += arg.get();
//        }
    }


//    数据整合
    @Override
    public void merge(Writable buffer, Writable partial) throws UDFException {
        AvgBuffer buf = (AvgBuffer) buffer;
        AvgBuffer p = (AvgBuffer) partial;
        buf.sum += p.sum;
        buf.count += p.count;
    }

    //    结果输出
    @Override
    public Writable terminate(Writable buffer) throws UDFException {
        AvgBuffer buf = (AvgBuffer) buffer;
        if (buf.count == 0) {
            ret.set(0);
        } else {
            ret.set(buf.sum);
            ret.set(buf.sum / buf.count);
        }
        return ret;
    }



}