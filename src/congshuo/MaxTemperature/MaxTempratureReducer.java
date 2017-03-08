package congshuo.MaxTemperature;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//经过shuffel操作，mapper传进来的数据类型是 <year,{tem1,tem2,tem3...}>
//keyIN:string ；ValueIN：string
//keyOut:string ValueOut:string
public class MaxTempratureReducer extends Reducer<Text, Text, Text, Text>{
	//reduce阶段是将shuffel阶段的 类型中将value中温度最大的数据提取出来
	@Override
	protected void reduce(Text key1, Iterable<Text> value1, Context context)
			throws IOException, InterruptedException {
		double Max=0.0;
		double t=0.0;
		String st;
		for(Text text:value1)
		{
			st=text.toString();
			t=Double.parseDouble(st);
			if(t>Max)  //寻找最大的温度
			{
				Max=t;
			}
		}
		st=Double.toString(Max);                  //调用Double.toString 函数将Max转换为string类型
		context.write(key1, new Text(st));            //将<year,Max>输出
	}
}
