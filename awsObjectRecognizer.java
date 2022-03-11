package com.example.awsObjectRecognizer;

import java.io.IOException;
import java.util.*;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.rekognition.model.DetectLabelsRequest;
import software.amazon.awssdk.services.rekognition.model.DetectLabelsResponse;
import software.amazon.awssdk.services.rekognition.RekognitionClient;
import software.amazon.awssdk.services.rekognition.model.Image;
import software.amazon.awssdk.services.rekognition.model.Label;
import software.amazon.awssdk.services.rekognition.model.S3Object;

import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.S3Client;

import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

public class App {

    private static Region region = Region.US_EAST_1;
    private static String bucketName = "my_s3_bucket_name";
    private static String queueName = "my_sqs_queue_name.fifo";
    private static String targetLabel = "my_target_label_name";

    public static void main(String[] args) throws IOException {

        S3Client s3 = S3Client.builder()
                .region(region)
                .build();

        SqsClient sqsClient = SqsClient.builder()
                .region(region)
                .build();

        String queueUrl = createSQSQueue(sqsClient);
        if(!queueUrl.isEmpty())
        {
            checkS3Bucket(s3, sqsClient, queueUrl);
        }

        s3.close();
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
                    sqsClient.getQueueUrl(GetQueueUrlRequest.builder().queueName(queueName).build());
            String queueUrl = getQueueUrlResponse.queueUrl();
            return queueUrl;

        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
        }
        return "";
    }

    private static void sendMessage(SqsClient sqsClient, String queueUrl, String message)
    {
        System.out.println("Sending message => " + message);
        try {
            SendMessageRequest sendMsgRequest = SendMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .messageGroupId("Default")
                    .messageBody(message)
                    .build();

            sqsClient.sendMessage(sendMsgRequest);
        }
        catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
        }
    }

    private static void checkS3Bucket(S3Client s3Client, SqsClient sqsClient, String queryUrl)
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

                software.amazon.awssdk.services.rekognition.model.S3Object object = S3Object.builder().bucket(bucketName).name(photo_key).build();
                Image myImage = Image.builder().s3Object(object).build();
                DetectLabelsRequest detectLabelsRequest = DetectLabelsRequest.builder()
                        .image(myImage)
                        .maxLabels(10)
                        .minConfidence(75F)
                        .build();

                DetectLabelsResponse result1 = rekognitionClient.detectLabels(detectLabelsRequest);
                List<Label> labels = result1.labels();

                Hashtable<String, Integer> numbers = new Hashtable<String , Integer>();
                for (Label label : labels)
                {
                    if(label.name().equals(targetLabel) & label.confidence()>=80)
                    {
                        System.out.print("Detected labels for:  " + photo_key +" => ");
                        numbers.put(label.name(), Math.round(label.confidence()));
                        System.out.print("Label: " + label.name() + " ,");
                        System.out.print("Confidence: " + label.confidence().toString() + "\n");

                        sendMessage(sqsClient, queryUrl, photo_key);
                    }
                }
            }
            sendMessage(sqsClient, queryUrl, "-1");
        }
        catch (Exception ex)
        {
            System.err.println(ex.getMessage());
            sendMessage(sqsClient, queryUrl, "-1");
        }
    }
}