package com.nuix.exercise.service;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;


@Service
public class S3Service {

    private static final Logger logger = LoggerFactory.getLogger(S3Service.class);

    @Value("${aws.region}")
    public String awsRegion;
    @Value("${aws.s3BucketName}")
    public String s3BucketName;

    private final AmazonS3 s3Client = AmazonS3Client.builder().withRegion("ap-southeast-2").build();


    public List<String> listFiles(String s3Prefix, String extFilter) {
        logger.info("Listing files in s3 bucket " + this.s3BucketName + " for prefix " + s3Prefix);
        List<String> s3KeyList = new ArrayList<>();
        ListObjectsV2Result listing = s3Client.listObjectsV2(s3BucketName, s3Prefix);
        for (S3ObjectSummary summary : listing.getObjectSummaries()) {
            if (summary.getKey().endsWith(extFilter)) {
                s3KeyList.add(summary.getKey());
            }
        }
        return s3KeyList;
    }

    public void downloadFile(String s3FilePath, String localPath) {
        logger.info("Starting download of file " + s3FilePath + " in S3 bucket " + s3BucketName);
        TransferManager tsManager = TransferManagerBuilder.standard().withMultipartUploadThreshold(1024L * 1024).
                withS3Client(s3Client).build();
        try {
            File file = new File(localPath);
            Download download = tsManager.download(s3BucketName, s3FilePath, file);
            download.waitForCompletion();
            logger.info("Finished download of file " + s3FilePath);
        } catch (AmazonClientException | InterruptedException e) {
            logger.error("Error downloading file: " + e.getLocalizedMessage(), e);
        } finally {
            tsManager.shutdownNow(false);
        }
    }

    public void uploadFile(String filePath, String s3Prefix) {
        logger.info("Starting upload of file " + filePath + " to s3 bucket " + s3BucketName);
        TransferManager tsManager = TransferManagerBuilder.standard().withMultipartUploadThreshold(1024L * 1024).
                withS3Client(s3Client).build();

        File file = new File(filePath);
        try {
            String s3KeyName = s3Prefix + Path.of(filePath).getFileName().toString();
            Upload upload = tsManager.upload(s3BucketName, s3KeyName, file);
            upload.waitForCompletion();
            logger.info("Finished upload of file " + filePath);
        } catch (AmazonClientException | InterruptedException e) {
            logger.error("Error uploading file: " + e.getLocalizedMessage(), e);
        } finally {
            tsManager.shutdownNow(false);
        }
    }
}
