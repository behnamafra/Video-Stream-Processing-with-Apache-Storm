package org.example;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.opencv.core.Core;


//author:  Behnam Afrasiyabi       ID: 401725002
public class VideoStreaming {
    static {
        System.loadLibrary(Core.NATIVE_LIBRARY_NAME);
    }

    public static void main(String[] args) throws Exception {
        runStormTopology();
    }
    public static void runStormTopology() throws Exception {
        // Build Topology
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("Spout", new FrameSpouts(), 2);
        builder.setBolt("gaussian-blur-bolt", new GaussianBlurBolt(), 2).shuffleGrouping("Spout");
        builder.setBolt("sharpening-bolt", new SharpeningBolt(), 2).shuffleGrouping("Spout");
        builder.setBolt("Receive-bolt", new ReceiveProcessedFrameBolt(), 2)
                .shuffleGrouping("sharpening-bolt")
                .shuffleGrouping("gaussian-blur-bolt");
        // Configuration
        Config conf = new Config();
        // Run Topology Locally
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("video-processing-topology", conf, builder.createTopology());
        Thread.sleep(100000);
        cluster.shutdown();
    }

}





