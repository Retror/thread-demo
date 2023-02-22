package com.moap.dataprocess.thread;

import com.hyt.common.NormalData;
import com.moap.dataprocess.service.ProcessService;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.Callable;

@Slf4j
public class TaskCallable implements Callable<NormalData> {
    private String dataUrl;
    private ProcessService processService;

    public TaskCallable(String dataUrl, ProcessService processService) {
        this.dataUrl = dataUrl;
        this.processService = processService;
    }

    @Override
    public NormalData call() throws Exception {
        long begin = System.currentTimeMillis();
        NormalData normalData = processService.getNormalData(dataUrl);
        long end = System.currentTimeMillis();
//        System.out.println("-----"+Thread.currentThread().getName()+"获取数据成功,用时:"+(end-begin));
        long time = end-begin;
        log.info("线程名称:{},获取数据成功,用时:{},数据长度:{}",Thread.currentThread().getName(),time,normalData.data.length+"-"+normalData.data[0].length);
        return normalData;
    }
}
