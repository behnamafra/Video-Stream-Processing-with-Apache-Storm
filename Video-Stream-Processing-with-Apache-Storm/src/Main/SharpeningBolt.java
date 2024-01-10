package org.example;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.opencv.core.Core;
import org.opencv.core.Mat;
import org.opencv.core.Size;
import org.opencv.imgproc.Imgproc;

import java.util.Map;

class SharpeningBolt extends BaseRichBolt {
    private OutputCollector collector;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        // Extract the frame from the tuple
        Mat originalFrame = (Mat) tuple.getValueByField("frame");
        String originalFrameName = tuple.getStringByField("originalFrameName");
        Integer TotalFrames = tuple.getIntegerByField("TotalFrames");
        Mat sharpenedFrame = new Mat();
        Imgproc.GaussianBlur(originalFrame, sharpenedFrame, new Size(0, 0), 3);
        Core.addWeighted(sharpenedFrame, 1.5, sharpenedFrame, -0.5, 0, sharpenedFrame);
        collector.emit(new Values(originalFrame, originalFrameName, TotalFrames));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("SharpFrame", "FrameNameSharp", "TotalFrames"));
    }
}
