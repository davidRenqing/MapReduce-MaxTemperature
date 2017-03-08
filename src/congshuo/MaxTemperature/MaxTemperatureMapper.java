package congshuo.MaxTemperature;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	//写Map函数，将文本的数据进行清洗。提取出year 和 temperature数据。使用context进行封装
	@Override
	protected void map(LongWritable key0, Text value0, Context context)
			throws IOException, InterruptedException {
		String line=value0.toString();                 //得到文本当中的一行
		String [] fields=StringUtils.split(line, ",");           //老师是使用string类型将一行数据切分成一个个字段。和我的按照char类型的字符串进行切割有什么区别呢
		
		String year=fields[0];                  //得到年的信息
		String temperature=fields[3];              //得到温度的信息
		
		//将年和温度作为context 的输出，year 作为key，temperature 作为value
		context.write(new Text(year),new Text(temperature));
		
	}

}
