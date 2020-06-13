package com.sks.minioclient;

import io.minio.MinioClient;
import io.minio.errors.InvalidEndpointException;
import io.minio.errors.InvalidPortException;

public final class MinIOClientProvider {
    private static MinIOClientProvider instance = null;
    private MinioClient minioClient;

    private MinIOClientProvider() {
    }

    void initClient() throws InvalidEndpointException, InvalidPortException {
        minioClient = new MinioClient("localhost", 9000, "minioadmin", "minioadmin", false);

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
