package com.refactorlabs.cs378.assign1;
//import com.refactorlabs.cs378.utils.Utils;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.*;
import java.util.Map;
import java.util.StringTokenizer;


//import com.sun.xml.internal.ws.client.sei.SEIStub;
import javafx.beans.binding.MapExpression;
import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;

import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RawKeyValueIterator;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.jobhistory.Events;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * Created by DilanHira on 9/10/16.
 */


class EventComparator implements Comparator<Event> {
    public int compare(Event a, Event b) throws ArrayIndexOutOfBoundsException{

        int strCmp = a.getEventTime().toString().compareTo(b.getEventTime().toString());
        if(strCmp == 0)
            strCmp = a.getEventType().toString().compareTo(b.getEventType().toString());

        return strCmp;
    }
}

class WordComparator implements Comparator<AvroValue<Session>> {
    public int compare(AvroValue<Session> a, AvroValue<Session> b) throws ArrayIndexOutOfBoundsException{
        int strCmp = a.datum().getEvents().get(0).getEventTime().toString().compareTo(
                b.datum().getEvents().get(0).getEventTime().toString());
        if(strCmp == 0){
            strCmp = a.datum().getEvents().get(0).getEventType().toString().compareTo(
                    b.datum().getEvents().get(0).getEventType().toString());
        }
        return strCmp;

    }
}

public class WordStatistics extends Configured implements Tool{

    public WordStatistics(){

    }


    public static EventType getEventType(String evTp){
        if(evTp.equals("visit")){
            return EventType.VISIT;
        }
        else if (evTp.equals("change"))
            return EventType.CHANGE;
        else if(evTp.equals("click"))
            return EventType.CLICK;
        else if(evTp.equals("display"))
            return EventType.DISPLAY;
        else if(evTp.equals("edit"))
            return EventType.EDIT;
        else
            return EventType.SHOW;
    }

    public static EventSubtype getEventSubType(String subTp){
        if(subTp.equals("contact form")){
            return EventSubtype.CONTACT_FORM;
        }
        else if (subTp.equals("alternative"))
            return EventSubtype.ALTERNATIVE;
        else if(subTp.equals("contact button"))
            return EventSubtype.CONTACT_BUTTON;
        else if(subTp.equals("features"))
            return EventSubtype.FEATURES;
        else if(subTp.equals("get directions"))
            return EventSubtype.GET_DIRECTIONS;
        else if(subTp.equals("vehicle history"))
            return EventSubtype.VEHICLE_HISTORY;
        else if(subTp.equals("Badge Detail"))
            return EventSubtype.BADGE_DETAIL;
        else if(subTp.equals(("photo modal")))
            return EventSubtype.PHOTO_MODAL;
        else if(subTp.equals("badges"))
                return EventSubtype.BADGES;
        else
            return EventSubtype.MARKET_REPORT;
    }

    public static VehicleCondition getVehicleCondition(String vc){
        if(vc.equals("new"))
            return VehicleCondition.New;
        else
            return VehicleCondition.Used;
    }

    public static cab_style getCabStyle(String bs){
        if(bs.equals("Crew Cab"))
            return cab_style.Crew;
        else if(bs.equals("Regular Cab"))
            return cab_style.Regular;
        else
            return cab_style.Extended;
    }



    public static boolean getCarfax(String tf){
        if(tf.equals("t"))
            return true;
        else
            return false;
    }

    // returns Null if string is blank
    /*public static String check(String str){
        if(str.equals(""))
            return null;
        return str;
    }*/

    //public static WordStatisticsWritable ONE = new WordStatisticsWritable(1, 0, 0, 0, 0);
    public static class WordCountMapper extends Mapper<LongWritable, Text, Text, AvroValue<Session>> {

        public WordCountMapper(){}

        /**
         * Local variable "word" will contain the word identified in the input.
         */
        private Text word = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();
            String[] elements = line.split("\t");
            //StringTokenizer tokenizer = new StringTokenizer(line, "\t");

            //context.getCounter(Utils.MAPPER_COUNTER_GROUP, "Input Lines").increment(1L);


            Session.Builder builder = Session.newBuilder();

            String eventType = elements[1].substring(0, elements[1].indexOf(" "));
            String eventSubType = elements[1].substring(elements[1].indexOf(" ")+1);

            /*Create and populate event */
            Event ev = new Event();
            ev.setEventType(getEventType(eventType.toLowerCase()));
            ev.setEventSubtype(getEventSubType(eventSubType.toLowerCase()));
            if(!elements[4].equals("") && !elements[4].equals("null"))
                ev.setEventTime(elements[4]);
            if(!elements[2].equals("") && !elements[2].equals("null"))
                ev.setPage(elements[2]);
            if(!elements[3].equals("") && !elements[3].equals("null"))
                ev.setReferringDomain(elements[3]);
            if(!elements[5].equals("") && !elements[5].equals("null"))
                ev.setCity(elements[5]);
            if(!elements[6].equals("") && !elements[6].equals("null"))
                ev.setVin(elements[6]);
            ev.setVehicleCondition(getVehicleCondition(elements[7].toLowerCase()));
            ev.setYear(Long.parseLong(elements[8]));
            if(!elements[9].equals("") && !elements[9].equals("null"))
                ev.setMake(elements[9]);
            if(!elements[10].equals("") && !elements[10].equals("null"))
                ev.setModel(elements[10]);
            if(!elements[11].equals("") && !elements[11].equals("null"))
                ev.setTrim(elements[11]);
            ev.setBodyStyle(BodyStyle.valueOf(elements[12]));
            if(!elements[11].equals("") && !elements[11].equals("null"))
                ev.setCabStyle(getCabStyle(elements[13]));
            ev.setPrice(Double.parseDouble(elements[14]));
            ev.setMileage(Long.parseLong(elements[15]));
            ev.setImageCount(Long.parseLong(elements[16]));
            ev.setFreeCarfaxReport(getCarfax(elements[17]));

            if(!elements[18].equals("") && !elements[18].equals("null"))
            {
                String[] fet = elements[18].split(":");
                Arrays.sort(fet);
                List<CharSequence> features = new ArrayList<CharSequence>(Arrays.asList(fet));

                ev.setFeatures(features);
            }


            List<Event> lst = new ArrayList<Event>(Arrays.asList());

            lst.add(ev);
            builder.setEvents(lst);
            builder.setUserId(elements[0]);
            word.set(elements[0]);
            context.write(word, new AvroValue<Session>(builder.build()));

            /*HashMap<String, Long> words = new HashMap<>();
            while(tokenizer.hasMoreTokens()){
                String nextWord = tokenizer.nextToken().toLowerCase();
                if(words.containsKey(nextWord)){
                    words.put(nextWord, words.get(nextWord) + 1L);
                }
                else{
                    words.put(nextWord, 1L);
                }
            }*/

            /*Set<Map.Entry<String, Long>> entrySet = words.entrySet();
            for(Map.Entry<String, Long> entry : entrySet){
                word.set(entry.getKey());

                Session.Builder builder2 = Session.newBuilder();
                builder.setDocumentCount(1L);
                builder.setTotalCount(entry.getValue());
                builder.setMax(Long.MIN_VALUE);
                builder.setMin(Long.MAX_VALUE);
                builder.setMean(0);
                builder.setSumOfSquares((long) Math.pow(entry.getValue(), 2.0));
                builder.setVariance(0.0);
                //WordStatisticsWritable temp = new WordStatisticsWritable(1L, entry.getValue(), Math.pow(entry.getValue(), 2.0), 0.0);
                context.write(word, new AvroValue<Session>(builder2.build()));
                //context.getCounter(MAPPER_COUNTER_GROUP, "Words Out").increment(1L);
            }*/


            // For each word in the input line, emit a count of 1 for that word.
            /*while (tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken());

                WordStatisticsData.Builder builder = WordStatisticsData.newBuilder();
                //builder.setCount(Utils.ONE);
                context.write(word, new AvroValue<WordStatisticsData>(builder.build()));
                context.getCounter(Utils.MAPPER_COUNTER_GROUP, "Input Words").increment(1L);
            }*/
        }
    }





    public static class ReduceClass
            extends Reducer<Text, AvroValue<Session>,
            AvroKey<CharSequence>, AvroValue<Session>> {

        @Override
        public void reduce(Text key, Iterable<AvroValue<Session>> values, Context context)
                throws IOException, InterruptedException {

            // Need to remove duplicates and sort


            //context.getCounter(Utils.REDUCER_COUNTER_GROUP, "Words Out").increment(1L);
            ArrayList<Event> events = new ArrayList<>();
            for (AvroValue<Session> value : values) {
                for(Event e : value.datum().getEvents())
                    events.add(e);
            }

            HashSet<Event> uniqEvents = new HashSet<>();
            // Sum up the counts for the current word, specified in object "key".
            for (Event value : events) {
                uniqEvents.add(value);

            }

            ArrayList<Event> sess = new ArrayList<Event>();
            Iterator a = uniqEvents.iterator();
            while(a.hasNext()){
                sess.add((Event)a.next());
            }


            Collections.sort(sess, new EventComparator());



            // Emit the total count for the word.
            Session.Builder builder = Session.newBuilder();
            builder.setUserId(key.toString());
            builder.setEvents(sess);


            context.write(new AvroKey<CharSequence>(key.toString()),
                    new AvroValue<Session>(builder.build()));
        }
    }





    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: WordCountB <input path> <output path>");
            return -1;
        }

        Configuration conf = getConf();

        // Use this JAR first in the classpath (We also set a bootstrap script in AWS)
        conf.setBoolean(MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);

        Job job = Job.getInstance(conf, "WordStatistics");
        String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        // Identify the JAR file to replicate to all machines.
        job.setJarByClass(WordStatistics.class);

        // Specify the Map
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(WordCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        AvroJob.setMapOutputValueSchema(job, Session.getClassSchema());

        // Specify the Reduce
        //job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputFormatClass(AvroKeyValueOutputFormat.class);
        job.setReducerClass(ReduceClass.class);
        AvroJob.setOutputKeySchema(job, Schema.create(Schema.Type.STRING));
        AvroJob.setOutputValueSchema(job, Session.getClassSchema());

        // Grab the input file and output directory from the command line.
        String[] inputPaths = appArgs[0].split(",");
        for ( String inputPath : inputPaths ) {
            FileInputFormat.addInputPath(job, new Path(inputPath));
        }
        FileOutputFormat.setOutputPath(job, new Path(appArgs[1]));

        // Initiate the map-reduce job, and wait for completion.
        job.waitForCompletion(true);

        return 0;
    }

    public static void printClassPath() {
        ClassLoader cl = ClassLoader.getSystemClassLoader(); URL[] urls = ((URLClassLoader) cl).getURLs(); System.out.println("classpath BEGIN"); for (URL url : urls) {
            System.out.println(url.getFile());
        } System.out.println("classpath END"); System.out.flush();
    }


    public static void main(String[] args) throws Exception {
        //Utils.printClassPath();
        int res = ToolRunner.run(new WordStatistics(), args);
        System.exit(res);
    }
}

/*
com.refactorlabs.cs378.assign1.WordStatistics
s3://assignmenttwobucket/data/dataSet2.txt
s3://assignmenttwobucket/output/Assign2Run14
 */


// schema has a isEquals method
// for timestamp, can just compare string
