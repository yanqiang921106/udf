package DTO;

import com.aliyun.odps.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Administrator on 2017\12\19 0019.
 */
public class myWritable implements Writable {

    private double sum = 0;

    public double getSum() {
        return sum;
    }

    public void setSum(double sum) {
        this.sum = sum;
    }

    public void readFields(DataInput in) throws IOException {

    }

    public void write(DataOutput out) throws IOException {

    }
}
