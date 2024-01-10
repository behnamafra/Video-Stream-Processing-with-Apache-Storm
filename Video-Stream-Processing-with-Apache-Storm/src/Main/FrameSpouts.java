package org.example;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.opencv.core.Core;
import org.opencv.core.Mat;
import org.opencv.core.Scalar;
import org.opencv.core.Size;
import org.opencv.imgcodecs.Imgcodecs;
import org.opencv.imgproc.Imgproc;
import org.opencv.videoio.VideoCapture;

import java.io.*;
import java.util.Map;

class FrameSpouts implements IRichSpout, Serializable {
    private static final long serialVersionUID = 1L;

    private SpoutOutputCollector collector;
    private String framesFolder;
    private File[] frameFiles;
    private int currentIndex = 0;

    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        capture_and_process_video();
        framesFolder = "src/GrayAndResizeFrames/";  // Replace with the actual path to your frames folder
        frameFiles = new File(framesFolder).listFiles();
        System.out.println("TotalFrame" + frameFiles.length);
        this.collector = collector;
    }

    @Override
    public void close() {

    }

    @Override
    public void activate() {

    }

    @Override
    public void deactivate() {

    }

    @Override
    public void nextTuple() {
        if (currentIndex < frameFiles.length) {
            Mat frame = Imgcodecs.imread(frameFiles[currentIndex].getPath());
            String framePath = frameFiles[currentIndex].getAbsolutePath();
            String originalFrameName = new File(framePath).getName();
            if (!frame.empty()) {

                collector.emit(new Values((frame.clone()), originalFrameName, frameFiles.length));
            }
            currentIndex++;
            Utils.sleep(100); // Adjust the sleep duration based on your requirements
        }
    }

    @Override
    public void ack(Object o) {
        // Acknowledgment logic if needed
        String msgId = null;
        System.out.println("Tuple acknowledged: " + msgId);
    }

    @Override
    public void fail(Object o) {
        // Failure logic if needed
        String msgId = null;
        System.out.println("Tuple failed: " + msgId);
        // You might consider emitting the tuple again here if needed
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("frame", "originalFrameName", "TotalFrames"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
    public static void capture_and_process_video() {
        VideoCapture cap = new VideoCapture("src/Video.mp4");
        Mat frame = new Mat();
        int frameCount = 0;
        double totalBrightness = 0;
        // Output folder path
        String grayAndResizeFolder = "src/GrayAndResizeFrames/";
        String rawFramesFolder = "src/RawFrames/";

        // Create the output folders
        File outputFolderGrayAndResize = new File(grayAndResizeFolder);
        outputFolderGrayAndResize.mkdirs();

        File outputFolderRawFrames = new File(rawFramesFolder);
        outputFolderRawFrames.mkdirs();


        try (BufferedWriter writer = new BufferedWriter(new FileWriter("src/AverageBrightnessBefore.txt"))) {
            while (cap.read(frame)) {
                frameCount++;
                String outputFilerawframesPath = rawFramesFolder + frameCount + ".jpg";
                Imgcodecs.imwrite(outputFilerawframesPath, frame);

                // Convert the frame to grayscale
                Mat grayFrame = new Mat();
                Imgproc.cvtColor(frame, grayFrame, Imgproc.COLOR_BGR2GRAY);

                // Resize the grayscale frame
                Mat resizedFrame = new Mat();
                Imgproc.resize(grayFrame, resizedFrame, new Size(300, 300));  // Change the size as needed

                // Calculate the average brightness of the frame
                Scalar avgBrightness = Core.mean(resizedFrame);
                totalBrightness += avgBrightness.val[0];

                // Save the frame as an image file
                String outputFilegrayAndResizePath = grayAndResizeFolder + frameCount + ".jpg";
                Imgcodecs.imwrite(outputFilegrayAndResizePath, resizedFrame);

                // Write the average brightness to the text file
                writer.write("Frame " + frameCount + ": " + avgBrightness.val[0] + "\n");
            }
            double averageBrightness = totalBrightness / frameCount;
            System.out.println("The number of frames in this video is: " + frameCount);
            System.out.println("Average Brightness is: " + averageBrightness);

        } catch (IOException e) {
            e.printStackTrace();
        }
        saveBrightnessToFile(totalBrightness / frameCount, frameCount);
    }
    private static void saveBrightnessToFile(double averageBrightness, int totalframes) {
        String filePath = "src/AverageBrightnessBefore.txt";
        System.out.println("Average Brightness wrote in the file:");
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath, true))) {
            // Write average brightness and total frame count to the file
            writer.write("Average Brightness: " + averageBrightness + "\n");
            writer.write("Total Frames: " + totalframes + "\n");
            writer.write("\n");
            System.out.println("dfddfddfdfdfd:");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
