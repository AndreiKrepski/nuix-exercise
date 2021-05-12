package com.nuix.exercise.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

@Service
public class ETLService {

    private static final Logger logger = LoggerFactory.getLogger(FileService.class);

    @Value("${etl.filter}")
    public String filter;
    @Autowired
    private S3Service s3Service;
    @Autowired
    private FileService fileService;
    @Autowired
    private ParquetFormatService parquetService;

    String tmpDir = System.getProperty("java.io.tmpdir");

    public void process(List<String> s3FilePathList) {
        if (s3FilePathList.isEmpty()) {
            s3FilePathList.addAll(s3Service.listFiles("", ".zip"));
        }
        for (String s3FilePath : s3FilePathList) {
            String localFilePath = tmpDir + File.separator + s3FilePath;
            s3Service.downloadFile(s3FilePath.trim(), localFilePath);
            processZipFile(localFilePath);
        }
    }

    public void processZipFile(String zipFilePath) {
        logger.info("Starting processing zip file " + zipFilePath);
        try {
            String destDir = zipFilePath.substring(0, zipFilePath.lastIndexOf("."));
            fileService.removeDirectory(destDir);
            fileService.unzip(zipFilePath, destDir);
            Set<String> fileToProcess = fileService.listFiles(destDir, ".csv");
            List<String> outputFilePathList = new ArrayList<>();
            for (String filePath : fileToProcess) {
                outputFilePathList.add(parquetService.transformToParquet(filePath, filter));
            }
            outputFilePathList.forEach(filePath -> s3Service.uploadFile(filePath, ""));
            fileService.removeFile(zipFilePath);
            fileService.removeDirectory(destDir);
            logger.info("Finished processing of zip file " + zipFilePath);
        } catch (Exception e) {
            logger.error("Exception happened while processing zip file " + zipFilePath + ": " + e.getLocalizedMessage(), e);
        }
    }
}
