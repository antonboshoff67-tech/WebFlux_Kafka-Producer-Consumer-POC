package com.antontech.webflux_kafka.flink.jobs;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;

/**
 * Minimal, dependency-free Flink "Hello World" style job (previously named
 * {@code FlinkJobSimpleSample}). It has no connection to Kafka, MS SQL
 * Server or MySQL and exists purely as a lightweight smoke test to confirm
 * that a Flink {@link StreamExecutionEnvironment} can be created and
 * executed in this application's runtime/classpath.
 * <p>
 * Useful as a quick sanity check when troubleshooting Flink runtime issues
 * before attempting to run the real {@link MssqlItemToKafkaJob} or
 * {@link KafkaItemToMysqlJob} pipelines.
 * <p>
 * Kick off via {@code POST /flink/start-simple-job}.
 */
@Slf4j
public class FlinkWordStreamDemoJob
{
    /**
     * Runs a trivial two-stage map pipeline over an in-memory list of words
     * and prints the result to stdout/logs.
     *
     * @param args unused; present only to match the conventional Flink job entry point signature.
     * @throws Exception if the Flink environment fails to execute the job graph.
     */
    public static void main(String[] args) throws Exception
    {
        log.debug("Starting FlinkWordStreamDemoJob");
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        List<String> elements = Arrays.asList("Hello", "World", "Flink");
        DataStream<String> stream = env.fromData(elements);

        stream.map(new MapFunction<String, String>()
        {
            @Override
            public String map(String value)
            {
                log.debug("Original Value: {}", value);
                return value;
            }
        });

        DataStream<String> processedStream = stream.map(new MapFunction<String, String>()
        {
            @Override
            public String map(String value)
            {
                String result = "Processed: " + value;
                log.debug("Processed Value: {}", result);
                return result;
            }
        });

        processedStream.print();

        try
        {
            env.execute("FlinkWordStreamDemoJob");
            log.debug("FlinkWordStreamDemoJob executed successfully.");
        }
        catch (Exception e)
        {
            log.error("Execution failed for FlinkWordStreamDemoJob: {}", e.getMessage(), e);
        }
    }
}

