package com.sks.minioclient;

import io.minio.MinioClient;
import io.minio.errors.InvalidEndpointException;
import io.minio.errors.InvalidPortException;

public final class MinIOClientProvider {
    private final boolean localHostedBucket = true;
    private static MinIOClientProvider instance = null;
    private MinioClient minioClient;

    private MinIOClientProvider() {
    }

    void initClient() throws InvalidEndpointException, InvalidPortException {
        if (localHostedBucket) {
            minioClient = new MinioClient("localhost", 9000, "minioadmin", "minioadmin", false);
        } else {
            // For AWS or Oracle S3 connectivity
            minioClient = new MinioClient("https://XXX", "XXX", "XXX");
        }
    }

    MinioClient getClient() {
        return minioClient;
    }

    public static MinIOClientProvider getInstance() throws InvalidEndpointException, InvalidPortException {
        if (instance != null) {
            return instance;
        }
        synchronized (MinIOClientProvider.class) {
            if (instance == null) {
                MinIOClientProvider minioInstance = new MinIOClientProvider();
                minioInstance.initClient();
                instance = minioInstance;
            }
            return instance;
        }
    }
}
