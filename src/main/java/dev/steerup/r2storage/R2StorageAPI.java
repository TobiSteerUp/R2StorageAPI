package dev.steerup.r2storage;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;

public class R2StorageAPI {

    private final AwsClientBuilder.EndpointConfiguration endpointConfiguration;
    private final AWSStaticCredentialsProvider awsStaticCredentialsProvider;
    private final String bucket;

    private AmazonS3 client;

    private R2StorageAPI(String apiUrl, String accessKey, String secretKey, String bucket) {
        this.endpointConfiguration = new AwsClientBuilder.EndpointConfiguration(apiUrl, "auto");
        this.awsStaticCredentialsProvider = new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey));
        this.bucket = bucket;
        this.initialize();
    }

    public void initialize() {
        client = AmazonS3ClientBuilder.standard()
                .withEndpointConfiguration(this.endpointConfiguration)
                .withCredentials(this.awsStaticCredentialsProvider)
                .build();
    }

    public void deleteFile(String identifier) {
        this.client.deleteObject(new DeleteObjectRequest(this.bucket, identifier));
    }

    public void uploadFile(String identifier, File file) {
        this.client.putObject(new PutObjectRequest(this.bucket, identifier, file));
    }

    public void downloadFile(String identifier, File file) {
        S3Object object = this.client.getObject(new GetObjectRequest(this.bucket, identifier));
        try (InputStream inputStream = object.getObjectContent()) {
            try (OutputStream outputStream = Files.newOutputStream(file.toPath())) {
                byte[] buffer = new byte[1024];
                int bytesRead;
                while ((bytesRead = inputStream.read(buffer)) > 0) {
                    outputStream.write(buffer, 0, bytesRead);
                }
            }
        } catch (IOException exception) {
            exception.printStackTrace();
        }
    }

    public ObjectMetadata getMetadata(String identifier) {
        return this.client.getObject(new GetObjectRequest(this.bucket, identifier)).getObjectMetadata();
    }

    public static R2StorageAPI create(String apiUrl, String accessKey, String secretKey, String bucket) {
        return new R2StorageAPI(apiUrl, accessKey, secretKey, bucket);
    }
}
