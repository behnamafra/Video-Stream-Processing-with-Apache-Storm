package org.example;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.opencv.core.Mat;
import org.opencv.core.Size;
import org.opencv.imgproc.Imgproc;

import java.util.HashMap;
import java.util.Map;

class GaussianBlurBolt extends BaseRichBolt {
    private OutputCollector collector;
    private Map<Long, String> tupleToFrameName = new HashMap<>();

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {

        this.collector = collector;
    }

    public void execute(Tuple tuple) {
        // Extract the frame from the tuple
        Mat originalFrame = (Mat) tuple.getValueByField("frame");
        String originalFrameName = tuple.getStringByField("originalFrameName");
        Integer TotalFrames = tuple.getIntegerByField("TotalFrames");
        // Apply GaussianBlur
        Mat processedFrame = new Mat();
        Imgproc.GaussianBlur(originalFrame, processedFrame, new Size(45, 45), 0);
        // Emit the processed frame to the next bolt
        collector.emit(tuple, new Values(processedFrame, originalFrameName, TotalFrames));
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("BlurFrame", "FrameNameBlur", "TotalFrames"));
    }

}
