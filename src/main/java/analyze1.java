

/*pseudo code:

class mapper {
   map(line, text){
   emit( station, [ (min temp, 1 or 0), (max temp, 1 or 0) ] )
   }

}
class reducer{
   reduce(station, [ ..., [ (min temp, 1or0), (max temp, 1or0) ], ...]
     emit(station, mean min temp, min max temp

}

class combiner{
   reduce(station, [,,,[ ( min temp, 1or0), (max temp, 1or0) ]...] )
   emit(station, [,,,[ ( min temp, count), (max temp, count) ]...] )

}
1) Please push source code,build files(pom and make) and readme in the git. .class files and other ide related file should not be pushed in the git. There will be deduction of marks starting next time.﻿﻿﻿﻿﻿

2) Please commit the code in regular intervals. Code committed in non regular interval can loose marks from next time.

3) Please try to follow oops concept, modularization and naming  convention while writing your code.
*/






import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.*;
import java.io.*;
 class NumPair implements Writable { //define a data structure to store some data
         int Count_max; float Sum_max;
         int Count_min; float Sum_min;
        NumPair() {}
        public void write(DataOutput out) throws IOException {
            out.writeInt(Count_max);out.writeFloat(Sum_max);
            out.writeInt(Count_min);out.writeFloat(Sum_min);
        }
        public void readFields(DataInput in) throws IOException {
            Count_max = in.readInt();Sum_max = in.readFloat();
            Count_min = in.readInt();Sum_min = in.readFloat();

        }

    }

public class analyze1 {
/*    public static class NumPair implements Writable { //define a data structure to store some data
        private int Count_max;private float Sum_max;
        private int Count_min;private float Sum_min;
        NumPair() {}
        public void write(DataOutput out) throws IOException {
            out.writeInt(Count_max);out.writeFloat(Sum_max);
            out.writeInt(Count_min);out.writeFloat(Sum_min);
        }
        public void readFields(DataInput in) throws IOException {
            Count_max = in.readInt();Sum_max = in.readFloat();
            Count_min = in.readInt();Sum_min = in.readFloat();

        }

    }*/

    public static class myMapper
            extends Mapper<Object, Text, Text, NumPair>{

        private  NumPair pair = new NumPair();
        private Text station = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = value.toString();
            List<String> line2=new ArrayList<String>(Arrays.asList(line.split(",")));
            pair.Count_max=0;pair.Sum_max=0;pair.Count_min=0;pair.Sum_min=0;
            if( line2.get(2).equals("TMAX") ) {
                station=new Text(line2.get(0)); pair.Count_max+=1;
                pair.Sum_max+=Float.parseFloat(line2.get(3));
                context.write(station,pair);
            }
            else if (line2.get(2).equals("TMIN") ) {
                station=new Text(line2.get(0)); pair.Count_min+=1;
                pair.Sum_min+=Float.parseFloat(line2.get(3));
                context.write(station,pair);}
            else {}

        }
    }

    public static class combiner
            extends Reducer<Text,NumPair,Text,NumPair> {
        private NumPair result = new NumPair();

        public void reduce(Text key, Iterable<NumPair> values,
                           Context context
        ) throws IOException, InterruptedException {
            result.Count_max=0;result.Count_min = 0;
            result.Sum_max=0;result.Sum_min=0;
            for (NumPair val : values) {
                result.Count_max+=val.Count_max;result.Sum_max+=val.Sum_max;
                result.Count_min+=val.Count_min;result.Sum_min+=val.Sum_min;
                //sum += val.get();
            }
            context.write(key, result);
        }
    }

    public static class MeanReducer
            extends Reducer<Text,NumPair,Text,Text> {
        private Text result = new Text();

        public void reduce(Text key, Iterable<NumPair> values,
                           Context context
        ) throws IOException, InterruptedException {
            int count_max=0;int count_min = 0;
            float sum_max=0;float sum_min=0;
            for (NumPair val : values) {
                count_max+=val.Count_max; sum_max+=val.Sum_max;
                count_min+=val.Count_min; sum_min+=val.Sum_min;
                //sum += val.get();
            }
            String maxString, minString;
            if (count_max==0){maxString="this station has no tmax record";}
            else{float maxMean=sum_max/count_max; maxString=Float.toString(maxMean);}
            if (count_min==0){minString="this station has no tmin record";}
            else{float minMean=sum_min/count_min; minString=Float.toString(minMean);}
            result=new Text(minString+","+maxString);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "myjob");
        job.setJarByClass(analyze1.class);
        job.setMapperClass(myMapper.class);
        job.setCombinerClass(combiner.class);
        job.setReducerClass(MeanReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NumPair.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

