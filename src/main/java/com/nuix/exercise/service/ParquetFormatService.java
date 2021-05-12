package com.nuix.exercise.service;

import com.nuix.exercise.parquet.CustomParquetWriter;
import com.opencsv.CSVReader;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

@Service
public class ParquetFormatService {

    public String transformToParquet(String csvFilePath, String filter) throws Exception {
        String outputFilePath = csvFilePath.substring(0, csvFilePath.lastIndexOf(".")) + ".parquet";
        try (CSVReader reader = new CSVReader(new FileReader(csvFilePath))) {
            String[] lineInArray;
            String[] headers = reader.readNext();
            MessageType schema = buildSchemaForParquetFile(headers);
            try (CustomParquetWriter writer = getParquetWriter(schema, outputFilePath)) {
                while ((lineInArray = reader.readNext()) != null) {
                    if (Arrays.toString(lineInArray).contains(filter)) {
                        writer.write(Arrays.asList(lineInArray));
                    }
                }
            }
        }
        return outputFilePath;
    }

    private CustomParquetWriter getParquetWriter(MessageType schema, String outputFilePath) throws IOException {
        File outputParquetFile = new File(outputFilePath);
        org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(outputParquetFile.toURI().toString());
        return new CustomParquetWriter(
                path, schema, false, CompressionCodecName.SNAPPY
        );
    }

    private MessageType buildSchemaForParquetFile(String[] firstRow) throws IOException {
        List<Type> fields = new ArrayList<>();
        String[] headers = isCsvWithHeader(firstRow) ? firstRow : generateHeaders(firstRow);
        for (String header : headers) {
            Type type = new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.BINARY, header.replace("\uFEFF", ""));
            fields.add(type);
        }
        return new MessageType("parquet_schema", fields);
    }

    private boolean isCsvWithHeader(String[] headers) {
        return Arrays.stream(headers).map(String::trim).noneMatch(NumberUtils::isCreatable);
    }

    private String[] generateHeaders(String[] firstRow) {
        return IntStream.range(0, firstRow.length)
                .mapToObj(i -> "Column" + i).toArray(String[]::new);
    }
}
