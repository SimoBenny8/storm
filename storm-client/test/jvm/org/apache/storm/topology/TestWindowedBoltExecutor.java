package org.apache.storm.topology;

import java.io.File;
import java.util.*;

import org.apache.storm.Config;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.generated.Grouping;
import org.apache.storm.task.GeneralTopologyContext;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

@RunWith(Parameterized.class)
public class TestWindowedBoltExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(TestWindowedBoltExecutor.class);
    private String fieldName;
    private long[] timestamps;
    private String componentId;
    private String srcComponent;
    private Integer timeoutSec;
    private Integer windowsLengthDuration;
    private Integer slidingIntervalDuration;
    private Integer tupleTimestampMaxLag;
    private Integer waterMarkEventInterval;
    private Integer taskId;
    private Map<String,Optional<?>> restoreState;

    public TestWindowedBoltExecutor(TestInput ti) {
        this.fieldName = ti.getFieldName();
        this.timestamps = ti.getTimestamps();
        this.componentId = ti.getComponentId();
        this.srcComponent = ti.getSrcComponent();
        this.timeoutSec = ti.getTimeoutSec();
        this.windowsLengthDuration = ti.getWindowsLengthDuration();
        this.slidingIntervalDuration = ti.getSlidingIntervalDuration();
        this.tupleTimestampMaxLag = ti.getTupleTimestampMaxLag();
        this.waterMarkEventInterval = ti.getWaterMarkEventInterval();
        this.taskId = ti.getTaskId();
        this.restoreState = ti.getRestoreState();

    }

    @Parameterized.Parameters
    public static Collection<TestInput[]> getTestParameters() {
        Collection<TestInput> inputs = new ArrayList<>();

        Collection<TestInput[]> result = new ArrayList<>();

        Map<String,Optional<?>> state = new HashMap<>();
        state.put("ts", Optional.of(123L));

        long[] timeStamps = new long[]{240,2049,2944,29484,2992};

        inputs.add(new TestInput("ts",timeStamps,"source","s1",10000,9999,30,50,30,1, state));
        for (TestInput e : inputs) {
            result.add(new TestInput[] { e });
        }
        return result;

    }


    @Test
    public void testExecuteWithTs() {

        MockWindowedBolt mockWindowedBolt = new MockWindowedBolt();
        mockWindowedBolt.withTimestampField(this.fieldName); //"ts"
        WindowedBoltExecutor executor = new WindowedBoltExecutor(mockWindowedBolt);
        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, this.timeoutSec); //10000
        conf.put(Config.TOPOLOGY_BOLTS_WINDOW_LENGTH_DURATION_MS, this.windowsLengthDuration);
        conf.put(Config.TOPOLOGY_BOLTS_SLIDING_INTERVAL_DURATION_MS, this.slidingIntervalDuration);
        conf.put(Config.TOPOLOGY_BOLTS_TUPLE_TIMESTAMP_MAX_LAG_MS, this.tupleTimestampMaxLag);
        // trigger manually to avoid timing issues
        conf.put(Config.TOPOLOGY_BOLTS_WATERMARK_EVENT_INTERVAL_MS, this.waterMarkEventInterval);

        TopologyContext context = mock(TopologyContext.class);

        Map<GlobalStreamId, Grouping> sources = Collections.singletonMap(new GlobalStreamId(this.componentId, "default"), null);
        Mockito.when(context.getThisSources()).thenReturn(sources);

        executor.prepare(conf, context, mock(OutputCollector.class));

        TopologyBuilder builder = new TopologyBuilder();
        GeneralTopologyContext gtc = new GeneralTopologyContext(builder.createTopology(),
                new Config(), new HashMap(), new HashMap(), new HashMap(), "") {
            @Override
            public Fields getComponentOutputFields(String componentId, String streamId) {
                return new Fields(fieldName);
            }

        };


        //long[] timestamps = { 603, 605, 607, 618, 626, 636 };
        for (long ts : timestamps) {
            executor.execute(new TupleImpl(gtc,new Values(ts), this.srcComponent,this.taskId,this.componentId){
                @Override
                public GlobalStreamId getSourceGlobalStreamId() {
                    return new GlobalStreamId(componentId, "default");
                }
            });
        }

        executor.waterMarkEventGenerator.run();

        assertTrue( mockWindowedBolt.tupleWindows.size() > 0);
        LOG.info(String.valueOf(mockWindowedBolt.tupleWindows.size()));
        TupleWindow first = mockWindowedBolt.tupleWindows.get(0);
        LOG.info(first.toString());
        assertArrayEquals(new long[]{this.timestamps[0]}, new long[]{(long) first.get().get(0).getValue(0)});

        assertNotNull(executor.getState());

        executor.restoreState(this.restoreState);

        if(!this.restoreState.equals(null)){
            assertEquals(this.restoreState.get("ts"),executor.getState().get("ts"));
        }

        executor.cleanup();
    }

    private static class TestInput {

        private String fieldName;
        private long[] timestamps;
        private String componentId;
        private String srcComponent;
        private Integer timeoutSec;
        private Integer windowsLengthDuration;
        private Integer slidingIntervalDuration;
        private Integer tupleTimestampMaxLag;
        private Integer waterMarkEventInterval;
        private Integer taskId;
        private Map<String,Optional<?>> restoreState;


        public TestInput(String fieldName, long[] timestamps, String componentId, String srcComponent, Integer timeoutSec, Integer windowsLengthDuration, Integer slidingIntervalDuration, Integer tupleTimestampMaxLag, Integer waterMarkEventInterval, Integer taskId, Map<String, Optional<?>> restoreState) {
            this.fieldName = fieldName;
            this.timestamps = timestamps;
            this.componentId = componentId;
            this.srcComponent = srcComponent;
            this.timeoutSec = timeoutSec;
            this.windowsLengthDuration = windowsLengthDuration;
            this.slidingIntervalDuration = slidingIntervalDuration;
            this.tupleTimestampMaxLag = tupleTimestampMaxLag;
            this.waterMarkEventInterval = waterMarkEventInterval;
            this.taskId = taskId;
            this.restoreState = restoreState;
        }

        public String getFieldName() {
            return fieldName;
        }

        public long[] getTimestamps() {
            return timestamps;
        }

        public String getComponentId() {
            return componentId;
        }

        public String getSrcComponent() {
            return srcComponent;
        }

        public Integer getTimeoutSec() {
            return timeoutSec;
        }

        public Integer getWindowsLengthDuration() {
            return windowsLengthDuration;
        }

        public Integer getSlidingIntervalDuration() {
            return slidingIntervalDuration;
        }

        public Integer getTupleTimestampMaxLag() {
            return tupleTimestampMaxLag;
        }

        public Integer getWaterMarkEventInterval() {
            return waterMarkEventInterval;
        }

        public Integer getTaskId() {
            return taskId;
        }

        public Map<String, Optional<?>> getRestoreState() {
            return restoreState;
        }
    }


    private static class MockWindowedBolt extends BaseWindowedBolt {
       private List<TupleWindow> tupleWindows = new ArrayList<>();

        @Override
        public void execute(TupleWindow input) {

            tupleWindows.add(input);
        }
    }
}
