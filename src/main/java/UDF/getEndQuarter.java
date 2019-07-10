package UDF;

import com.aliyun.odps.udf.ExecutionContext;
import com.aliyun.odps.udf.UDF;
import com.aliyun.odps.udf.UDFException;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Created by Administrator on 2018\7\9 0009.
 * 取指定日期所在季的季末日期
 */
public class getEndQuarter extends UDF {
    public Date evaluate(Date inDate) throws ParseException {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(inDate);

        return getEndOfQuarter(calendar);
    }

    private Date getEndOfQuarter(Calendar calendar) throws ParseException {
        SimpleDateFormat simpleDateFormat=new SimpleDateFormat("yyyyMMdd");
        /*取日期所在年和月*/
        int year = calendar.get(Calendar.YEAR);
        int mon = calendar.get(Calendar.MONTH) + 1 ;

        /*穷举取季底日期*/
        if(mon >= 1 && mon <= 3){
            return simpleDateFormat.parse(String.valueOf(year)+"03"+"31");
        }

        if(mon >= 4 && mon <= 6){
            return simpleDateFormat.parse(String.valueOf(year)+"06"+"30");
        }

        if(mon >= 7 && mon <= 9){
            return simpleDateFormat.parse(String.valueOf(year)+"09"+"30");
        }

        if(mon >= 10 && mon <= 12){
            return simpleDateFormat.parse(String.valueOf(year)+"12"+"31");
        }

        return calendar.getTime();
    }

    @Override
    public void setup(ExecutionContext ctx) throws UDFException {
        super.setup(ctx);
    }

    @Override
    public void close() throws UDFException {
        super.close();
    }

}
