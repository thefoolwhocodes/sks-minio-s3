package com.sks.minioclient;

/**
 * The MinIOClient program demonstrates the sample usage of MinIO java client.
 * It also creates/manipulates s3 object exposed via a S3 compatible object
 * store.
 * 
 * @author Shashi Santosh
 * @version 1.0
 */
public class MinIOClient {
    public static void main(String[] args) {
        MinIOClientDemonstrator demonstrator = new MinIOClientDemonstrator();
        demonstrator.demonstrate();
    }
}
