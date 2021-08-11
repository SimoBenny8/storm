package org.apache.storm.topology;

import org.apache.storm.Config;
import org.apache.storm.spout.CheckPointState;
import org.apache.storm.state.State;
import org.apache.storm.task.GeneralTopologyContext;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

@RunWith(Parameterized.class)
public class TestStatefulBoltExecutor {

    private Map<String, Object> topoConf;
    private TopologyContext context;
    private OutputCollector collector;
    private State state;
    private Tuple checkpointTuple;
    private CheckPointState.Action action; //prepare,commit,rollback,initstate,null
    private long txid;
    private Tuple input;

    public TestStatefulBoltExecutor(TestInput ti) {

        this.topoConf = ti.getTopoConf();
        this.context = ti.getContext();
        this.collector = ti.getCollector();
        this.state = ti.getState();
        this.checkpointTuple = ti.getCheckpointTuple();
        this.action = ti.getAction();
        this.txid = ti.getTxid();
        this.input = ti.getInput();


    }

    @Parameterized.Parameters
    public static Collection<TestInput[]> getTestParameters() {

        Collection<TestInput> inputs = new ArrayList<>();

        Collection<TestInput[]> result = new ArrayList<>();

        ArrayList<Object> value = new ArrayList<>();
        value.add("test");

        TupleImpl tmpl = new TupleImpl(mock(GeneralTopologyContext.class),value ,"src",1,"strId");
        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.TOPOLOGY_STATE_PROVIDER,"org.apache.storm.hbase.state.HBaseKeyValueStateProvider");
        conf.put(Config.TOPOLOGY_STATE_PROVIDER_CONFIG, "{" +
                "   \"hbaseConfigKey\": \"hbase.conf\"," +
                "   \"tableName\": \"state\"," +
                "   \"columnFamily\": \"cf\"" +
                " }");

        inputs.add(new TestInput(conf,mock(TopologyContext.class),mock(OutputCollector.class),mock(State.class),tmpl, CheckPointState.Action.INITSTATE,30L,tmpl));
        for (TestInput e : inputs) {
            result.add(new TestInput[] { e });
        }
        return result;

    }


    @Test
    public void testExecutor(){

        MockStatefulBolt iBolt = new MockStatefulBolt();

        StatefulBoltExecutor sbe = new StatefulBoltExecutor(iBolt);
        sbe.prepare(this.topoConf,this.context,this.collector,this.state);

        assertEquals(this.topoConf,iBolt.topoConfOut);


        sbe.handleCheckpoint(this.checkpointTuple,this.action,this.txid);

        if(this.action.equals(CheckPointState.Action.COMMIT) || this.action.equals(CheckPointState.Action.PREPARE))
            assertEquals(this.txid,iBolt.txidOut);

        sbe.handleTuple(this.input);

        assertEquals(this.input,iBolt.getOutput());

        sbe.cleanup();
    }


    private static class TestInput {

        private Map<String, Object> topoConf;
        private TopologyContext context;
        private OutputCollector collector;
        private State state;
        private Tuple checkpointTuple;
        private CheckPointState.Action action;
        private long txid;
        private Tuple input;


        public TestInput(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector, State state, Tuple checkpointTuple, CheckPointState.Action action, long txid, Tuple input) {
            this.topoConf = topoConf;
            this.context = context;
            this.collector = collector;
            this.state = state;
            this.checkpointTuple = checkpointTuple;
            this.action = action;
            this.txid = txid;
            this.input = input;
        }

        public Map<String, Object> getTopoConf() {
            return topoConf;
        }

        public TopologyContext getContext() {
            return context;
        }

        public OutputCollector getCollector() {
            return collector;
        }

        public State getState() {
            return state;
        }

        public Tuple getCheckpointTuple() {
            return checkpointTuple;
        }

        public CheckPointState.Action getAction() {
            return action;
        }

        public long getTxid() {
            return txid;
        }

        public Tuple getInput() {
            return input;
        }
    }


    private class MockStatefulBolt implements IStatefulBolt {

        private Tuple output;
        private long txidOut;
        private Map topoConfOut;
        private State stateOut;

        @Override
        public void prepare(Map topoConf, TopologyContext context, OutputCollector collector) {
            topoConfOut = topoConf;
        }

        @Override
        public void execute(Tuple input) {
            output = input;
        }

        @Override
        public void cleanup() {

        }

        @Override
        public void initState(State state) {
            stateOut = state;
        }

        @Override
        public void preCommit(long txid) {
            txidOut = txid;
        }

        @Override
        public void prePrepare(long txid) {
           txidOut = txid;
        }

        @Override
        public void preRollback() {

        }

        public Tuple getOutput() {
            return output;
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {

        }

        @Override
        public Map<String, Object> getComponentConfiguration() {
            return null;
        }
    }
}
