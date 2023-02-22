package com.moap.dataprocess.util;

import java.io.BufferedOutputStream;
import java.io.IOException;

/**
 * 文件操作工具类
 */
public class FileUtil {
    public static boolean writeDataToFile(BufferedOutputStream bufferedOutputStream,byte[] data){
        try {
            bufferedOutputStream.write(data);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }
}
