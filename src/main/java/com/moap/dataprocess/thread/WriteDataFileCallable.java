package com.moap.dataprocess.thread;

import com.hyt.common.NormalData;
import com.moap.dataprocess.util.CommonUtils;
import lombok.extern.slf4j.Slf4j;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.Callable;

@Slf4j
public class WriteDataFileCallable implements Callable<Boolean> {
    private String filePath;
    private long skip;
    private NormalData normalData;

    public WriteDataFileCallable(String filePath, long skip, NormalData normalData) {
        this.filePath = filePath;
        this.skip = skip;
        this.normalData = normalData;
    }

    @Override
    public Boolean call() throws Exception {
        log.info("开始写入数据文件...");
        log.info("偏移量:{}",skip);
        long begin = System.currentTimeMillis();
        RandomAccessFile raf = null;
        try {
//            raf = new RandomAccessFile("D://abc.txt", "rw");
            raf = new RandomAccessFile(filePath, "rw");
            raf.seek(skip);
            float[][] data = normalData.data;
            for (int i = 0; i < data.length; i++) {
                byte[] content = CommonUtils.floatArrayToByteArray(data[i]);
                raf.write(content);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return false;
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return false;
        } finally {
            try {
                raf.close();
            } catch (Exception e) {
                return false;
            }
        }
        long end = System.currentTimeMillis();
        log.info("写入数据文件成功,用时:{}",end-begin);
        return true;
    }
}
