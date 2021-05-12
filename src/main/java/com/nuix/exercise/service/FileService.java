package com.nuix.exercise.service;

import net.lingala.zip4j.ZipFile;
import net.lingala.zip4j.exception.ZipException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.*;
import java.nio.file.*;
import java.util.*;

@Service
public class FileService {

    private static final Logger logger = LoggerFactory.getLogger(FileService.class);


    public void unzip(String srcZipPath, String destDirPath) {
        unzip(srcZipPath, destDirPath, Optional.empty());
    }

    public void unzip(String srcZipPath, String destDirPath, Optional<String> passwordOpt) {
        try {
            ZipFile zipFile = new ZipFile(srcZipPath);
            if (zipFile.isEncrypted()) {
                passwordOpt.ifPresent(password -> zipFile.setPassword(password.toCharArray()));
            }
            zipFile.extractAll(destDirPath);
        } catch (ZipException e) {
            logger.error("Error unzipping file: " + e.getLocalizedMessage(), e);
        }
    }

    public Set<String> listFiles(String dir, String ext) throws Exception {
        Set<String> fileList = new HashSet<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(Paths.get(dir))) {
            for (Path path : stream) {
                String pathStr = path.toString().toLowerCase();
                if (!Files.isDirectory(path) & pathStr.endsWith(ext)) {
                    fileList.add(path.toString());
                }
            }
        }
        return fileList;
    }

    public boolean removeFile(String filePath) throws Exception {
        return new File(filePath).delete();
    }

    public void removeDirectory(String dirPath) throws Exception {
        if (new File(dirPath).exists()) {
            Files.walk(Path.of(dirPath))
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        } else {
            logger.warn("Could not remove directory which doesn't exist: " + dirPath);
        }
    }
}
