
/*pseudo code:
1.naive way
class mapper {
   map(line, text){
   emit( station, [ (min temp, 1 or 0), (max temp, 1 or 0) ] )
   }
}
class reducer{
   reduce(station, [ ..., [ (min temp, 1or0), (max temp, 1or0) ], ...]
     emit(station, mean min temp, min max temp
}
2 +combiner
class combiner{
   reduce(station, [,,,[ ( min temp, 1or0), (max temp, 1or0) ]...] )
   emit(station, [,,,[ ( min temp, count), (max temp, count) ]...] )
}
3 +in-mappper combine
class mapper {
   private hashmap H    //H: {...(station,[ min temp, count_min, max temp, count_max ]...}
   setup(){H=new hashmap}
   map(line, text){
   accumulate for H )
   }
   cleanup(){emit H}
}


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


public class analyze1 {

    public static class NumPair implements Writable { //define a data structure to store some data
        int Count_max; float Sum_max;
        int Count_min; float Sum_min;
        public NumPair() {

        }
        public void write(DataOutput out) throws IOException {
            out.writeInt(Count_max);out.writeFloat(Sum_max);
            out.writeInt(Count_min);out.writeFloat(Sum_min);
        }
        public void readFields(DataInput in) throws IOException {
            Count_max = in.readInt();Sum_max = in.readFloat();
            Count_min = in.readInt();Sum_min = in.readFloat();

        }

    }
    public static class myMapper
            extends Mapper<Object, Text, Text, NumPair>{

        private  NumPair pair = new NumPair();
        private Text station = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = value.toString();
            List<String> line2=new ArrayList<String>(Arrays.asList(line.split(",")));
            pair.Count_max=0;pair.Sum_max=0;pair.Count_min=0;pair.Sum_min=0;//reset for method
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

    public static class CombiningMapper
            extends Mapper<Object, Text, Text, NumPair>{

        //private  NumPair pair = new NumPair();
        //private Text station = new Text();
        private Map<String, NumPair> all;//combining structure

        protected void setup(Context context)
                throws IOException, InterruptedException{
            all=new HashMap<String, NumPair>();
        }
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = value.toString();
            List<String> line2=new ArrayList<String>(Arrays.asList(line.split(",")));
            //pair.Count_max=0;pair.Sum_max=0;pair.Count_min=0;pair.Sum_min=0;
            if( line2.get(2).equals("TMAX") ) {
                if( all.get(line2.get(0))==null ){
                    NumPair p=new NumPair();
                    p.Count_max=1; p.Sum_max=Float.parseFloat(line2.get(3)); p.Count_min=0;p.Sum_min=0;
                    all.put(line2.get(0),p);
                }
                else{ NumPair pair=all.get(line2.get(0));
                    float sum_max=Float.parseFloat(line2.get(3) )+pair.Sum_max;
                    int count_max=1+pair.Count_max;
                    NumPair p=new NumPair();
                    p.Count_max=count_max; p.Sum_max=sum_max; p.Count_min=pair.Count_min; p.Sum_min=pair.Sum_min;
                    all.put(line2.get(0),p);
                }
            }
            else if (line2.get(2).equals("TMIN") ) {
                if( all.get(line2.get(0))==null ){
                    NumPair p=new NumPair();
                    p.Count_max=1; p.Sum_max=0; p.Count_min=1;p.Sum_min=Float.parseFloat(line2.get(3));
                    all.put(line2.get(0),p);
                }
                else{ NumPair pair=all.get(line2.get(0));
                    NumPair p=new NumPair();
                    float sum_min=Float.parseFloat(line2.get(3) )+pair.Sum_min;
                    int count_min=1+pair.Count_min;
                    p.Count_max=pair.Count_max; p.Sum_max=pair.Sum_max; p.Count_min=count_min; p.Sum_min=sum_min;
                    all.put(line2.get(0),p);
                }
            }
            else {}

        }
        protected void cleanup(Context context)
                throws IOException, InterruptedException{
            for(String val: all.keySet()){
                context.write(new Text(val), all.get(val));
            }

        }
    }


    public static class combiner
            extends Reducer<Text,NumPair,Text,NumPair> {
        private NumPair result = new NumPair();

        public void reduce(Text key, Iterable<NumPair> values,
                           Context context
        ) throws IOException, InterruptedException {
            result.Count_max=0;result.Count_min = 0;//reset for every method call
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
        if(args[2].equals("inner_combine")) {job.setMapperClass(CombiningMapper.class);//select version
            System.out.println("inner_combine_yep");}
        else{job.setMapperClass(myMapper.class);}

        if(args[2].equals("combine")){
            job.setCombinerClass(combiner.class);System.out.println("combine_yep");}

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