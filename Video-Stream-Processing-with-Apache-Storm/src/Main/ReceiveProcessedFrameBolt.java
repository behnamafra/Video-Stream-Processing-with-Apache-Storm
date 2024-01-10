package org.example;

import org.apache.commons.io.FilenameUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.opencv.core.Core;
import org.opencv.core.Mat;
import org.opencv.core.Scalar;
import org.opencv.core.Size;
import org.opencv.imgcodecs.Imgcodecs;
import org.opencv.imgproc.Imgproc;
import org.opencv.videoio.VideoWriter;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;

class ReceiveProcessedFrameBolt extends BaseRichBolt {
    private OutputCollector collector;
    int CountFrame;
    String sharpenedFolderPath = "src/SharpenedFrame/";
    String gaussianBlurFolderPath = "src/GaussianBlurFrame/";
    String mergedFramesFolderPath = "src/MergedFrames/";
    String outputVideoPath = "src/FinalVideo/MergedVideo.avi";
    File[] sortedFrames;
    boolean lock = true;

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;

    }

    @Override
    public void execute(Tuple tuple) {
        if ((tuple.getFields().contains("BlurFrame")) && (tuple.getFields().contains("FrameNameBlur"))) {
            // This tuple is from GaussianBlurBolt
            Mat ProcessedFrameBlur = (Mat) tuple.getValueByField("BlurFrame");
            String originalFrameName = tuple.getStringByField("FrameNameBlur");
            Integer TotalFrames = tuple.getIntegerByField("TotalFrames");
            // Save the processed frames and create the video
            SaveFrame(ProcessedFrameBlur, originalFrameName, "1", TotalFrames);
            System.out.println("Processing blurred frame...");
            // Your processing logic for blurred frame
        } else if ((tuple.getFields().contains("SharpFrame")) && (tuple.getFields().contains("FrameNameSharp"))) {
            // This tuple is from SharpeningBolt
            Mat ProcessedFrameSharp = (Mat) tuple.getValueByField("SharpFrame");
            String originalFrameName = tuple.getStringByField("FrameNameSharp");
            Integer TotalFrames = tuple.getIntegerByField("TotalFrames");
            SaveFrame(ProcessedFrameSharp, originalFrameName, "0", TotalFrames);
            System.out.println("Processing sharpened frame...");
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    private void SaveFrame(Mat frame, String originalFrameName, String bit, Integer totalframe) {
        String processedFrameName = originalFrameName;
        String FolderPathAvgBrightness="src/AverageBrightnessAfter.txt";

        double totalBrightness = 0;
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(FolderPathAvgBrightness, true))){
            if (bit == "1") { //blur
                // Create the output folder if it doesn't exist
                File folderGaussian = new File(gaussianBlurFolderPath);
                if (!folderGaussian.exists()) {
                    folderGaussian.mkdirs();
                }
                // Construct the full path for saving the processed frame using the name of the original frame
                Path filePathGaussian = Paths.get(gaussianBlurFolderPath, processedFrameName);
                // Save the frame
                Imgcodecs HighGui = new Imgcodecs();
                HighGui.imwrite(filePathGaussian.toString(), frame);
                CountFrame += 1;
            } else if (bit == "0") { //sharp
                // Create the output folder if it doesn't exist
                File foldersharp = new File(sharpenedFolderPath);
                if (!foldersharp.exists()) {
                    foldersharp.mkdirs();
                }
                // Construct the full path for saving the processed frame using the name of the original frame
                Path filePathSharp = Paths.get(sharpenedFolderPath, processedFrameName);
                // Save the frame
                Imgcodecs HighGui = new Imgcodecs();
                HighGui.imwrite(filePathSharp.toString(), frame);
            }
            File sharpenedFolder = new File(sharpenedFolderPath);
            File gaussianBlurFolder = new File(gaussianBlurFolderPath);
            File mergedFramesFolder = new File(mergedFramesFolderPath);
            File[] sharpenedFiles = sharpenedFolder.listFiles();
            File[] gaussianBlurFiles = gaussianBlurFolder.listFiles();
            if ((sharpenedFiles.length == totalframe) && (gaussianBlurFiles.length == totalframe)) {
                if ((sharpenedFiles == null || sharpenedFiles.length == 0) && (gaussianBlurFiles == null || gaussianBlurFiles.length == 0)) {
                    System.err.println("Error: No frames found in the input folder.");
                    return;
                }
                // Create the output folder if it doesn't exist
                if (!mergedFramesFolder.exists()) {
                    mergedFramesFolder.mkdirs();
                }
                for (int i = 0; i < sharpenedFiles.length; i++) {
                    // Read the sharpened frame
                    System.err.println("Read the sharpened frame");
                    Mat sharpenedFrame = Imgcodecs.imread(sharpenedFiles[i].getAbsolutePath());
                    // Read the GaussianBlur frame
                    System.err.println("Read the GaussianBlur frame");
                    Mat gaussianBlurFrame = Imgcodecs.imread(gaussianBlurFiles[i].getAbsolutePath());

                    // Get the name of sharpened Frame
                    String framePath = sharpenedFiles[i].getAbsolutePath();
                    String mergedframesname = new File(framePath).getName();
                    System.err.println("Get the name of sharpened Frame");
                    // Merge the frames
                    Mat mergedFrame = mergeFrames(sharpenedFrame, gaussianBlurFrame);
                    System.err.println("Merge the frames");
                    // Save the merged frame
                    String mergedFramePath = mergedFramesFolderPath + mergedframesname;
                    Imgcodecs.imwrite(mergedFramePath, mergedFrame);
                    System.err.println("Save the merged frame");
                    // Resize the frame to the desired size
                    Imgproc.resize(mergedFrame, mergedFrame, new Size(300, 300));
                }
                File[] mergeFile = mergedFramesFolder.listFiles();

                if (mergeFile.length == totalframe) {
                    try {
                        // Sort frames based on numerical order
                        sortedFrames = sortFrames(mergedFramesFolderPath);
                        if (lock == true) {
                            int index=0;
                            for (File file : sortedFrames) {
                                // Calculate the average brightness of the frame
                                Mat FrametoSave = Imgcodecs.imread(file.getAbsolutePath());
                                Scalar avgBrightness = Core.mean(FrametoSave);
                                writer.write( "Frame"+String.format("%03d.jpg", index + 1) + ": " + avgBrightness.val[0] + "\n");
                                totalBrightness += avgBrightness.val[0];
                                index+=1;
                            }
                            saveBrightnessToFile(totalBrightness / totalframe,totalframe,lock);
                            lock = false;
                        }
                        // Create video from sorted frames
                        createVideoFromFrames(sortedFrames, outputVideoPath);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }

            }

        } catch (Exception e) {
            e.printStackTrace();
            // Handle the exception (e.g., log, throw, etc.)
        }

    }

    private static Mat mergeFrames(Mat frame1, Mat frame2) {
        Mat mergedFrame = new Mat();
        Core.addWeighted(frame1, 0.5, frame2, 0.5, 0, mergedFrame);
        return mergedFrame;
    }

    //Video creation function
    private static void createVideoFromFrames(File[] frames, String outputVideoPath) {
        if (frames.length == 0) {
            System.out.println("No frames found for video creation.");
            return;
        }

        int frameWidth = 300; // Set your frame width
        int frameHeight = 300; // Set your frame height
        double frameRate = 30; // Set your desired frame rate

        VideoWriter videoWriter = new VideoWriter(outputVideoPath, VideoWriter.fourcc('M', 'J', 'P', 'G'),
                frameRate, new Size(frameWidth, frameHeight), true);

        for (File file : frames) {
            Mat frame = Imgcodecs.imread(file.getAbsolutePath());
            if (frame.empty()) {
                System.out.println("Error loading image: " + file.getName());
                continue;
            }

            // Resize the frame to the desired size if needed
            if (frame.width() != frameWidth || frame.height() != frameHeight) {
                Imgproc.resize(frame, frame, new Size(frameWidth, frameHeight));
            }

            videoWriter.write(frame);
        }

        videoWriter.release();
        System.out.println("Video created successfully.");
    }

    private static File[] sortFrames(String inputFolderPath) {
        File folder = new File(inputFolderPath);
        File[] files = folder.listFiles();
        if (files == null || files.length == 0) {
            System.out.println("No image files found in the specified folder.");
            return new File[0];
        }

        // Sort files based on numerical order in the file names
        Arrays.sort(files, Comparator.comparing(file -> extractFrameNumber(file.getName())));

        // Create a new folder to store sorted frames
        File sortedFolder = new File("src/SortedFrames/");
        if (!sortedFolder.exists()) {
            sortedFolder.mkdirs();
        }

        // Copy sorted frames to the new folder
        for (int i = 0; i < files.length; i++) {
            File destinationFile = new File(sortedFolder, String.format("%03d.jpg", i + 1));
            files[i].renameTo(destinationFile);
        }

        return sortedFolder.listFiles();
    }

    private static int extractFrameNumber(String fileName) {
        // Extract numerical values from the file name
        String baseName = FilenameUtils.getBaseName(fileName);
        try {
            return Integer.parseInt(baseName);
        } catch (NumberFormatException e) {
            return 0; // Handle non-numeric filenames gracefully
        }
    }
    private void saveBrightnessToFile(double averageBrightness,int totalframes,boolean lock) {
        String filePath = "src/AverageBrightnessAfter.txt";
        System.out.println("ddsdsdsfhjhjhjhj");
        if (lock == true) {
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath, true))) {
                // Write average brightness and total frame count to the file
                writer.write("Average Brightness: " + averageBrightness + "\n");
                writer.write("Total Frames: " + totalframes + "\n");
                writer.write("\n");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
