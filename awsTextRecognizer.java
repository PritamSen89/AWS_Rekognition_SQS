package com.example.awsTextRecognizer;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

import software.amazon.awssdk.regions.Region;

import software.amazon.awssdk.services.rekognition.model.*;
import software.amazon.awssdk.services.rekognition.model.S3Object;
import software.amazon.awssdk.services.rekognition.RekognitionClient;

import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.S3Client;

import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

public class App {

    private static Region region = Region.US_EAST_1;
    private static String bucketName = "my_s3_bucket_name";
    private static String queueName = "my_sqs_queue_name.fifo";

    public static void main(String[] args) throws IOException {

        S3Client s3Client = S3Client.builder().
                region(region).
                build();
        SqsClient sqsClient = SqsClient.builder()
                .region(region)
                .build();

        String queueUrl = createSQSQueue(sqsClient);
        if(!queueUrl.isEmpty())
        {
             receiveMessage(s3Client, sqsClient, queueUrl);
        }

        s3Client.close();
        sqsClient.close();
        System.out.println("Connection closed");
        System.out.println("Exiting...");

    }

    private static String createSQSQueue(SqsClient sqsClient)
    {
        try {
            System.out.println("\nCreating fifo Queue");

            Map<String, String> attributes = new HashMap<String, String>();
            attributes.put("FifoQueue", "true");
            attributes.put("ContentBasedDeduplication", "true");
            CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                    .queueName(queueName)
                    .attributesWithStrings(attributes)
                    .build();

            sqsClient.createQueue(createQueueRequest);

            GetQueueUrlResponse getQueueUrlResponse =
                    sqsClient.getQueueUrl(GetQueueUrlRequest.builder().queueName(queueName)
                            .build());
            String queueUrl = getQueueUrlResponse.queueUrl();
            return queueUrl;

        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
        }
        return "";
    }

    private static void receiveMessage(S3Client s3Client, SqsClient sqsClient, String queueUrl) throws IOException {
        // Receive messages from the queue
        ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .waitTimeSeconds(10)
                .maxNumberOfMessages(10)
                .build();

        FileWriter fWriter = null;
        fWriter = getFileWriter();

        Boolean checkMsg = true;
        do{
            List<Message> messages = sqsClient.receiveMessage(receiveRequest).messages();
            System.out.println("Checking message");
            for (Message m : messages) {
                System.out.println("\n" +m.body());
                if(m.body().equals("-1"))
                {
                    checkMsg = false;
                    break;
                }
                else
                {
                    getText(s3Client, m.body(), fWriter);
                }
            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }while (checkMsg);

        fWriter.close();
    }

    private static FileWriter getFileWriter() throws IOException {
        String fileName = "results.txt";
        File fileObj = new File(fileName);
        if (fileObj.createNewFile()) {
            System.out.println("File created: " + fileObj.getName());
        }

        FileWriter fileWriter = new FileWriter(fileObj.getName());
        return fileWriter;
    }

    private static void getText(S3Client s3Client, String fileName, FileWriter fileWriter)
    {
        try {
            ListObjectsV2Request req = ListObjectsV2Request.builder().bucket(bucketName).build();
            ListObjectsV2Response response = s3Client.listObjectsV2(req);
            List<software.amazon.awssdk.services.s3.model.S3Object> objects = response.contents();

            RekognitionClient rekognitionClient = RekognitionClient.builder()
                    .region(region)
                    .build();

            for (ListIterator iterVals = objects.listIterator(); iterVals.hasNext(); ) {
                software.amazon.awssdk.services.s3.model.S3Object obs = (software.amazon.awssdk.services.s3.model.S3Object) iterVals.next();
                String photo_key = obs.key();

                if(photo_key.equals(fileName))
                {
                    software.amazon.awssdk.services.rekognition.model.S3Object object = S3Object.builder().bucket(bucketName).name(photo_key).build();
                    Image myImage = Image.builder().s3Object(object).build();
                    DetectTextRequest detectTextsRequest = DetectTextRequest.builder()
                            .image(myImage)
                            .build();

                    DetectTextResponse result1 = rekognitionClient.detectText(detectTextsRequest);
                    List<TextDetection> texts = result1.textDetections();

                    String photoKeyTitle = "================== " + photo_key + " ===================\r\n";
                    System.out.println(photoKeyTitle);
                    if(fileWriter != null)
                        fileWriter.write(photoKeyTitle);
                    for (TextDetection text : texts)
                    {
                        if(text.confidence() > 80F) {
                            String msgToPrint = "New Text Detected: " + text.detectedText() + " , Confidence: " + text.confidence().toString() + "\r\n";
                            System.out.print(msgToPrint);
                            if(fileWriter != null)
                                fileWriter.write(msgToPrint);
                        }
                    }
                }
            }
        }
        catch (Exception ex)
        {
            System.out.println(ex.getMessage());
        }
    }
}