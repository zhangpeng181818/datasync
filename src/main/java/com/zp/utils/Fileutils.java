package com.zp.utils;

import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

import java.io.*;

@Slf4j
public class Fileutils {

    public static String[] getTargetFileNames(String filePath) {
        Resource resource = new ClassPathResource(filePath);
        try {
            File file = resource.getFile();
            if (file.isDirectory()) {
                return file.list();
            }
        } catch (IOException e) {
            log.error("{}", e.getMessage(), e);
        }
        return null;
    }

    private static String read(String path) {
        File file = new File(path);
        StringBuilder res = new StringBuilder();
        String line = null;
        try {
            BufferedReader reader = new BufferedReader(new FileReader(file));
            while ((line = reader.readLine()) != null) {
                res.append(line + "\n");
            }
            reader.close();
        } catch (Exception e) {
            log.error("{}", e.getMessage(), e);
        }
        return res.toString();
    }

    private static boolean write(String cont, File dist) {
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(dist));
            writer.write(cont);
            writer.flush();
            writer.close();
            return true;
        } catch (IOException e) {
            log.error("{}", e.getMessage());
            return false;
        }
    }

    public static void replaceTargetStr(String filePath, String oldStr, String newsStr) {
        String read = read(filePath);
        write(read.replace(oldStr, newsStr), new File(filePath));
    }

    public static void main(String[] args) {
        String a = "[1,2,3,4]";
        System.out.println(a.replaceAll("\\[", "\"").replaceAll("\\]", "\""));
    }
}
