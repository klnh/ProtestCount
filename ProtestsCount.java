import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class ProtestCount {
    public static class ProtestCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(value, new IntWritable(1));
        }
    }

    public static class ProtestCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private String[] usStates = new String[]{
                "alabama","alaska","arizona","arkansas","california",
                "colorado","connecticut","delaware","florida","georgia",
                "hawaii","idaho","illinois","indiana","iowa","kansas",
                "kentucky","louisiana","maine","maryland","massachusetts",
                "michigan","minnesota","mississippi","missouri","montana",
                "nebraska","nevada","newhampshire","newjersey","newmexico",
                "newyork","northcarolina","northdakota","ohio","oklahoma",
                "oregon","pennsylvania","rhodeisland","southcarolina",
                "southdakota","tennessee","texas","utah","vermont","virginia",
                "washington","westvirginia","wisconsin","wyoming","dc"
        }; // 所有州的名称
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            boolean isState = false;
            String s = key.toString();
            for(String state : usStates){
                if(s.equals(state)){
                    isState = true;
                    break;
                }
            }
            if(!isState) // 如果不是州名，不统计
                return;

            Integer count = 0;
            for (IntWritable value : values) { //迭代统计
                count += value.get();
            }
            context.write(key, new IntWritable(count)); // 输出（州名，出现次数）
        }
    }
    public static void main(String[] args) throws Exception {

        // 创建配置对象
        Configuration conf = new Configuration();
        // 创建Job对象
        Job job = Job.getInstance(conf, "ProtestCount");
        // 设置运行Job的类
        job.setJarByClass(ProtestCount.class);
        // 设置Mapper类
        job.setMapperClass(ProtestCountMapper.class);
        // 设置Reducer类
        job.setReducerClass(ProtestCountReducer.class);
        // 设置Map输出的Key value
        job.setMapOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        // 设置Reduce输出的Key value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        // 设置输入输出的路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // 提交job
        boolean b = job.waitForCompletion(true);
        if(!b) {
            System.out.println("Wordcount task fail!");
        }
    }
}