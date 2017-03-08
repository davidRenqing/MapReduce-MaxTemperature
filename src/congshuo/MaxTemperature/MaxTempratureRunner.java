package congshuo.MaxTemperature;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MaxTempratureRunner extends Configured implements Tool{
	
	@Override
	public int run(String[] args) throws Exception {
		
		//创建新的conf文件
		Configuration conf=new Configuration();
		Job job = Job.getInstance(conf);                  //使用这个conf对象得到一个job
		job.setJarByClass(MaxTempratureRunner.class);      //设置main函数的所在的类
		
		//设置这个job运行的map类
		job.setMapperClass(MaxTemperatureMapper.class);
		job.setReducerClass(MaxTempratureReducer.class);             //设置reduce运行的类
		
		job.setMapOutputKeyClass(Text.class);               //设置map的key1 的类型
		job.setMapOutputValueClass(Text.class);             //设置map的value1的类型
		
		job.setOutputKeyClass(Text.class);                  //设置reduce的key的输出类型
		job.setOutputValueClass(Text.class);                //设置reduce的value的输出类型
		
		
		FileInputFormat.setInputPaths(job, new Path("hdfs://hadoop01:9000/user/hive/warehouse/wk110.db/myexpri"));       //设置文件的输入路径
		FileOutputFormat.setOutputPath(job, new Path("hdfs://hadoop01:9000/wc/output"));       //设置文件的输出路径
		
		
		
 		return job.waitForCompletion(true)?0:1;                       //如果 job.waitForCompletion 执行成功将返回true
		
	}
	
	public static void main(String[] args) throws Exception {
		int res=ToolRunner.run(new Configuration(), new MaxTempratureRunner(), args);
		System.exit(res);
	}
}
