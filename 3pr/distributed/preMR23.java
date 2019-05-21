/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Scanner;
import java.util.*;

public class preMR23 {

    public static int checkNumberCluster(String cluster) {
        String[] res = cluster.split("	");
        return (int) (Double.parseDouble(res[0]));
    }

    public static int binarySearch(String arr[], int x) {
        int l = 0, r = arr.length - 1;
        while (l <= r) {
            int m = l + (r - l) / 2;

            // Check if x is present at mid 
            if (checkNumberCluster(arr[m]) == x) {
                return m;
            }

            // If x greater, ignore left half 
            if (checkNumberCluster(arr[m]) < x) {
                l = m + 1;
            } // If x is smaller, ignore right half 
            else {
                r = m - 1;
            }
        }

        // if we reach here, then element was 
        // not present 
        return -1;
    }

    public static void main(String[] args) throws Exception {
        // read file midProcessRes 
        String[] res = new String[5];
            int i = 0;
        try {
            File file = new File(args[0]+"/res/midProcessRes");//should get hdfs FIX data/src/midProcessRes 
            Scanner sc = new Scanner(file);

            while (sc.hasNextLine()) {
                res[i] = sc.nextLine();
                i++;
            }
        } catch (FileNotFoundException e) {
        }

        // read cluster number lines 
        File file = new File(args[0]+"/res/result/result");//should get hdfs FIX /output 
        Scanner sc = new Scanner(file);

        // we just need to use \\Z as delimiter 
        sc.useDelimiter("\\Z");
        String[] lines = sc.next().split("\n");

        // Find cluster in file results
        // Get cluster in epsilon max: 2;3;4
        String[] maxClusters = res[2].split(";");
        // Get cluster in epsilon min: 2;3;4
        // Find location in file each cluster from maxCluster and get all cluster's doc infors . Assume that all cluster ordered  
        String fileContent = "";
        for (String cluster : maxClusters) {
            int location = binarySearch(lines, Integer.parseInt(cluster));
            if (location != -1) {
                fileContent = fileContent + lines[location] + "\n";
            }
        }

        writeToFile(args[0]+"/res/sentHDFS2", fileContent);

        // Find location in file each cluster from minCluster and get all cluster's doc infors 
        if (i != 4) {
            String[] minClusters = res[4].split(";");
            fileContent = "";
            for (String cluster : minClusters) {
                if (!cluster.isEmpty()) {

                    int location = binarySearch(lines, Integer.parseInt(cluster));
                    if (location != -1) {
                        fileContent = fileContent + lines[location] + "\n";
                    }
                }
            }

            writeToFile(args[0]+"/res/sentHDFS3", fileContent);
        }
    }

    private static void writeToFile(String path, String fileContent) {
        try {
            Files.write(Paths.get(path), fileContent.getBytes());
        } catch (Exception e) {
        }
    }
}

