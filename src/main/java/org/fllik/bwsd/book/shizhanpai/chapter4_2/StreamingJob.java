

package org.fllik.bwsd.book.shizhanpai.chapter4_2;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="https://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class StreamingJob {

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		 StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		 env.setParallelism(5);
		 DataStreamSource<Long>     longDataStreamSource = env.addSource(new MyParDataSource());
		 SingleOutputStreamOperator<Tuple1<Long>> map = longDataStreamSource.map(new MapFunction<Long, Tuple1<Long>>() {
			@Override
			public Tuple1<Long> map(Long value) throws Exception {
				return new Tuple1<>(value);
			}
		});
		 DataStream<Tuple1<Long>> tuple1DataStream = map.partitionCustom(new CustomPartition(), 0);
		 SingleOutputStreamOperator<Long> result = tuple1DataStream.map(new MapFunction<Tuple1<Long>, Long>() {
			@Override
			public Long map(Tuple1<Long> value) throws Exception {
				System.out.println("线程ID为:" + Thread.currentThread().getId() + ", value:" + value);
				return value.getField(0);
			}
		});
		result.print().setParallelism(2);
		env.execute("Flink Streaming Java API Skeleton");
	}



   public static class MyDataSource implements SourceFunction<Long> {


		private long count  = 0;

		@Override
		public void run(SourceContext<Long> sourceContext) throws Exception {
			while (true){
				sourceContext.collect(count);
				count++;
				Thread.sleep(1000L);
			}
		}

		@Override
		public void cancel() {

		}
	}

	public static class MyParDataSource implements ParallelSourceFunction<Long> {


		private long count  = 0;

		@Override
		public void run(SourceContext<Long> sourceContext) throws Exception {
			while (true){
				sourceContext.collect(count);
				count++;
				Thread.sleep(1000L);
			}
		}

		@Override
		public void cancel() {

		}
	}


	public static class CustomPartition implements Partitioner<Long>{

		@Override
		public int partition(Long aLong, int i) {
			if(aLong % 2 == 0){
				return 0;
			}else {
				return 1;
			}
		}
	}
}
