package com.moap.dataprocess.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hyt.common.LatLng;
import com.hyt.common.LatLngBounds;
import com.hyt.common.NormalData;
import com.moap.dataprocess.config.DataInfo;
import com.moap.dataprocess.config.ElementData;
import com.moap.dataprocess.config.SystemConfig;
import com.moap.dataprocess.service.ProcessService;
import com.moap.dataprocess.thread.TaskCallable;
import com.moap.dataprocess.thread.WriteDataFileCallable;
import com.moap.dataprocess.util.CommonUtils;
import com.moap.dataprocess.util.FileUtil;
import com.moap.dataprocess.util.HttpUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;

/**
 * 数据处理Controller
 */
@Slf4j
@RestController
public class ProcessController {

    @Autowired
    ProcessService processService;
    @Autowired
    SystemConfig systemConfig;
    @Autowired
    ElementData elementData;

    ThreadPoolExecutor threadPool = new ThreadPoolExecutor(5, 10, 100, TimeUnit.SECONDS, new LinkedBlockingQueue<>(1024 * 10));

    @GetMapping("/getData")
    public String getData() {
//        String url = "http://10.1.64.146/mdfs/v1.1/meta/list?dataPath=ECMWF_HR/HGT/500&type=mdfs&initTime=23021508";
        String url = systemConfig.moapListUrl + "?dataPath=ECMWF_HR/HGT/500&type=mdfs&initTime=23021508";
        try {
            String s = HttpUtils.get(url);
            System.out.println(s);
            System.out.println(processService.getTimeList().size());
            System.out.println(elementData.getDataInfos());

            DataInfo[] dataInfos = elementData.getDataInfos();
            String initTime="23022112";
            LocalDateTime initDt = LocalDateTime.parse(initTime, DateTimeFormatter.ofPattern("yyMMddHH"));
            //循环创建文件夹和文件
            for (int j = 0; j < dataInfos.length; j++) {

                String model = dataInfos[j].getModel();
                String element = dataInfos[j].getElement();

                String outDir = String.format("%s/%s/%d/%02d/%02d/%02d", "D:\\lz\\moapdata", model, initDt.getYear(), initDt.getMonthValue(), initDt.getDayOfMonth(), initDt.getHour());
                Files.createDirectories(Paths.get(outDir));
                String outFile = String.format("%s-%s-%s",
                        model,
                        DateTimeFormatter.ofPattern("yyyyMMddHH").format(initDt),
                        element
                );
                String metaFile = outDir + File.separator + outFile + "-meta.json";
                String binFile = outDir + File.separator + outFile + "-data.bin";
                //执行写操作

            }

        } catch (IOException e) {
            e.printStackTrace();
            return "fail";
        }
        return "success";
    }

    @GetMapping("/getNormalData")
    public NormalData getNormalData() {
        /**
         * 整体思路：
         * 分别读取不同模式下的不同时间段的数据，然后把所有数据写出成为一个大的文件
         * 并且也生成一个索引文件用来记录不同层次的数据的位置和其他信息
         */
//        String url = "http://10.1.64.146/mdfs/v1.1/data?dataPath=ECMWF_HR/HGT/500/23021508.240";
        String url = systemConfig.moapDataUrl + "?dataPath=ECMWF_HR/HGT/500/23021508.240";
        NormalData normalData = new NormalData();
        try {
            //通过http请求获取到数据,然后将数据写入到新的文件当中
            String s = HttpUtils.get(url);
            ObjectMapper objectMapper = new ObjectMapper();
            normalData = objectMapper.readValue(s, NormalData.class);
            //写入到新的文件当中
            if (normalData.data != null) {
                float[][] data = normalData.data;
                //数据总行数
                int y = data.length;
                //数据总列数
                int x = data[0].length;
                LatLngBounds bounds = normalData.bounds;
                LatLng northEast = bounds._northEast;
                LatLng southWest = bounds._southWest;
                File indexFilepath = new File(systemConfig.indexFilePath);
                File dataFilepath = new File(systemConfig.dataFilePath);
                if (!indexFilepath.exists()) {
                    indexFilepath.mkdirs();
                    log.info("存放索引文件的路径不存在,创建文件夹{}", indexFilepath);
                }
                if (!dataFilepath.exists()) {
                    dataFilepath.mkdirs();
                    log.info("存放数据文件的路径不存在,创建文件夹{}", dataFilepath);
                }
                //大数据文件索引文件路径
                String indexFilePath = systemConfig.indexFilePath + File.separator + "meta.json";
                //新生成的大数据文件路径
                String dataFilePath = systemConfig.dataFilePath + File.separator + "test.bin";
                File jsonFilePath = new File(indexFilePath);
                File binFilePath = new File(dataFilePath);
                FileOutputStream dataFileOutputStream = null;
                if (!binFilePath.exists()) {
                    binFilePath.createNewFile();
                    //没有文件就新建写入
                    dataFileOutputStream = new FileOutputStream(dataFilePath);
                } else {
                    //有文件就追加
                    dataFileOutputStream = new FileOutputStream(dataFilePath, true);
                }
                //索引文件输出流
                FileWriter writer = null;
                if (!jsonFilePath.exists()) {
                    //没有文件就新建写入
                    writer = new FileWriter(jsonFilePath);
                } else {
                    //有文件就追加
                    writer = new FileWriter(jsonFilePath, true);
                }
                //组合索引文件内容
                JSONObject object = new JSONObject();
                //组装最里面的数据内容
                JSONObject content = new JSONObject();
                content.put("position", 0);
                content.put("Y", y);
                content.put("X", x);
                content.put("startLat", southWest.lat);
                content.put("endLat", northEast.lat);
                content.put("startLon", southWest.lng);
                content.put("endLon", northEast.lng);
                content.put("res", normalData.res);
                //组装上一层数据
                JSONObject timekey = new JSONObject();
                timekey.put("23021508.240", content);
                //组装最外层数据
                object.put("HGT/500", timekey);
                //写入内容
                writer.append(object.toJSONString());
                writer.close();
                log.info("索引文件写入成功");
                //数据文件输出流
                BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(dataFileOutputStream);
                for (int i = 0; i < data.length; i++) {
                    byte[] bytes = CommonUtils.floatArrayToByteArray(data[i]);
                    //写出文件
                    FileUtil.writeDataToFile(bufferedOutputStream, bytes);
                }
                bufferedOutputStream.close();
                log.info("数据文件写出成功");
            }
        } catch (IOException e) {
            log.error("程序执行异常:{}", e.getMessage());
            return normalData;
        }
        return normalData;
    }

    @GetMapping("/writeJsonFile")
    public void writeJsonFile() {
        long begin = System.currentTimeMillis();
        //获取全部的模式列表集合
        DataInfo[] dataInfos = elementData.getDataInfos();
//        http://10.1.64.146/mdfs/v1.1/meta/list?dataPath=ECMWF_HR/HGT/500&type=mdfs&initTime=23021508
//        http://10.1.64.146/mdfs/v1.1/data?dataPath=ECMWF_HR/HGT/500/23021508.240
        StringBuffer buffer = new StringBuffer();
        //打开文件
        //大数据文件索引文件路径
        String indexFilePath = systemConfig.indexFilePath + File.separator + "meta.json";
        File jsonFilePath = new File(indexFilePath);
        //索引文件输出流
        FileWriter writer = null;
        try {
            if (!jsonFilePath.exists()) {
                //没有文件就新建写入
                writer = new FileWriter(jsonFilePath);
            } else {
                //有文件就追加
                writer = new FileWriter(jsonFilePath, true);
            }
            JSONObject jsonObject = new JSONObject();
            String dataFilePath = systemConfig.indexFilePath + File.separator + "-test.bin";
            for (int j = 0; j < dataInfos.length; j++) {
                List list = dataInfos[j].getList();
                long position = 0;
                for (int k = 0; k < list.size(); k++) {
                    String param = dataInfos[j].getModel() + "/" + list.get(k);
                    buffer.append(systemConfig.moapListUrl).append("?dataPath=").append(param).append("&type=mdfs&initTime=").append("23021508");
                    String url = buffer.toString();
                    List<String> result = new ArrayList<>();
                    String str = HttpUtils.get(url);
                    str = str.replace("[", "").replace("]", "").replace("\"", "").replace("\n", "");
                    //获取所有时间集合数据
                    result = Arrays.asList(str.split(","));
                    Map map = new TreeMap();
                    List<Future<NormalData>> futureList = new ArrayList<>();
                    for (String time : result) {
                        //读取数据，然后写入到大文件当中
                        System.out.println("param:" + param + "time:" + time + ".....");
                        String dataUrl = systemConfig.moapDataUrl + "?dataPath=" + param + "/" + time;
//                    NormalData normalData = processService.getNormalData(dataUrl);
//                    JSONObject object = CommonUtils.normalData2JsonObject(normalData);
//                    System.out.println(object.toJSONString());
//                    map.put(time,object);
                        //开启线程单独执行索引文件写入操作
                        Future<NormalData> future = threadPool.submit(new TaskCallable(dataUrl, processService));
                        futureList.add(future);
                    }
                    System.out.println("-----------");
                    for (int i = 0; i < futureList.size(); i++) {
                        Future<NormalData> dataFuture = futureList.get(i);
                        NormalData normalData = dataFuture.get();
                        //todo 1.如何记录position位置
                        //todo 2.如何写入大数据文件
                        int x = normalData.data[0].length;
                        int y = normalData.data.length;
                        int size = x * y * 4;
                        LatLngBounds bounds = normalData.bounds;
                        LatLng northEast = bounds._northEast;
                        LatLng southWest = bounds._southWest;
                        //组装最里面的数据内容
                        JSONObject content = new JSONObject();
                        content.put("position", position);
                        content.put("size", size);
                        content.put("Y", y);
                        content.put("X", x);
                        content.put("startLat", southWest.lat);
                        content.put("endLat", northEast.lat);
                        content.put("startLon", southWest.lng);
                        content.put("endLon", northEast.lng);
                        content.put("res", normalData.res);
                        position += size;
//                        JSONObject object = CommonUtils.normalData2JsonObject(normalData, i);
                        System.out.println("position=====>" + position);
                        map.put(result.get(i), content);
//                        System.out.println("~~~~~~~~"+dataFilePath);
                        threadPool.submit(new WriteDataFileCallable(dataFilePath, content.getLongValue("position"), normalData));
                    }
                    jsonObject.put(param, map);
                    buffer.setLength(0);
                }
//                System.out.println(position);
            }
            //写出文件
            writer.append(jsonObject.toJSONString());
            //关闭写出流
            writer.close();
            //关闭线程池
            threadPool.shutdown();
            long end = System.currentTimeMillis();
            System.out.println("索引文件写入完成...,用时:" + (end - begin));
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                if (writer != null) {
                    writer.close();
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }


    /**
     * 生成文件
     *
     * @param model
     * @param initTime
     * @param element
     */
    @GetMapping("/task")
    public void task(@RequestParam String model, @RequestParam String initTime, @RequestParam String element) {
        long begin = System.currentTimeMillis();
        //获取全部的模式列表集合
        DataInfo[] dataInfos = elementData.getDataInfos();
//        http://10.1.64.146/mdfs/v1.1/meta/list?dataPath=ECMWF_HR/HGT/500&type=mdfs&initTime=23021508
//        http://10.1.64.146/mdfs/v1.1/data?dataPath=ECMWF_HR/HGT/500/23021508.240
        StringBuffer buffer = new StringBuffer();
        //索引文件输出流
        FileWriter writer = null;
        LocalDateTime initDt = LocalDateTime.parse(initTime, DateTimeFormatter.ofPattern("yyMMddHH"));
        try {
            String outDir = String.format("%s/%s/%d/%02d/%02d/%02d", systemConfig.indexFilePath, model, initDt.getYear(), initDt.getMonthValue(), initDt.getDayOfMonth(), initDt.getHour());
            Files.createDirectories(Paths.get(outDir));
            String outFile = String.format("%s-%s-%s",
                    model,
                    DateTimeFormatter.ofPattern("yyyyMMddHH").format(initDt),
                    element
            );
            String metaFile = outDir + File.separator + outFile + "-meta.json";
            String binFile = outDir + File.separator + outFile + "-data.bin";
            File jsonFilePath = new File(metaFile);
            if (!jsonFilePath.exists()) {
                //没有文件就新建写入
                writer = new FileWriter(jsonFilePath);
            } else {
                //有文件就追加
                writer = new FileWriter(jsonFilePath, true);
            }
            JSONObject jsonObject = new JSONObject();
            for (int j = 0; j < dataInfos.length; j++) {
                List list = dataInfos[j].getList();
                long position = 0;
                for (int k = 0; k < list.size(); k++) {
                    String param = dataInfos[j].getModel() + "/" + list.get(k);
                    buffer.append(systemConfig.moapListUrl).append("?dataPath=").append(param).append("&type=mdfs&initTime=").append("23021508");
                    String url = buffer.toString();
                    //获取所有时间集合数据
                    List<String> result = new ArrayList<>();
                    String str = HttpUtils.get(url);
                    ObjectMapper objectMapper = new ObjectMapper();
                    result = objectMapper.readValue(str, List.class);
                    Map map = new TreeMap();
                    List<Future<NormalData>> futureList = new ArrayList<>();
                    for (String time : result) {
                        //读取数据，然后写入到大文件当中
                        System.out.println("param:" + param + "time:" + time + ".....");
                        String dataUrl = systemConfig.moapDataUrl + "?dataPath=" + param + "/" + time;
                        //开启线程单独执行索引文件写入操作
                        Future<NormalData> future = threadPool.submit(new TaskCallable(dataUrl, processService));
                        futureList.add(future);
                    }
                    System.out.println("-----------");
                    for (int i = 0; i < futureList.size(); i++) {
                        Future<NormalData> dataFuture = futureList.get(i);
                        NormalData normalData = dataFuture.get();
                        //todo 1.如何记录position位置
                        //todo 2.如何写入大数据文件
                        int x = normalData.data[0].length;
                        int y = normalData.data.length;
                        int size = x * y * 4;
                        LatLngBounds bounds = normalData.bounds;
                        LatLng northEast = bounds._northEast;
                        LatLng southWest = bounds._southWest;
                        //组装最里面的数据内容
                        JSONObject content = new JSONObject();
                        content.put("position", position);
                        content.put("size", size);
                        content.put("Y", y);
                        content.put("X", x);
                        content.put("startLat", southWest.lat);
                        content.put("endLat", northEast.lat);
                        content.put("startLon", southWest.lng);
                        content.put("endLon", northEast.lng);
                        content.put("res", normalData.res);
                        position += size;
                        System.out.println("position=====>" + position);
                        map.put(result.get(i), content);
//                        System.out.println("~~~~~~~~"+dataFilePath);
                        threadPool.submit(new WriteDataFileCallable(binFile, content.getLongValue("position"), normalData));
                    }
                    jsonObject.put(param, map);
                    buffer.setLength(0);
                }
            }
            //写出文件
            writer.append(jsonObject.toJSONString());
            //关闭写出流
            writer.close();
            //关闭线程池
            threadPool.shutdown();
            long end = System.currentTimeMillis();
            System.out.println("索引文件写入完成...,用时:" + (end - begin));
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                if (writer != null) {
                    writer.close();
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }


    /**
     * 读取数据文件当中的数据返回
     *
     * @param model
     * @param initTime
     * @param ele
     * @param level
     * @param lat
     * @param lon
     */
    @GetMapping("/timeseries")
    //timeseries?model=ECMWF_HR&initTime=2023021608&ele=HGT&level=500&lat=35.2&lon=120.1
    public String timeseries(@RequestParam String model, @RequestParam String initTime, @RequestParam String ele, @RequestParam String level, @RequestParam String lat, @RequestParam String lon) {
        LocalDateTime initDt = LocalDateTime.parse(initTime, DateTimeFormatter.ofPattern("yyMMddHH"));
        try {
            String outDir = String.format("%s/%s/%d/%02d/%02d/%02d", systemConfig.indexFilePath, model, initDt.getYear(), initDt.getMonthValue(), initDt.getDayOfMonth(), initDt.getHour());
            String outFile = String.format("%s-%s-%s",
                    model,
                    DateTimeFormatter.ofPattern("yyyyMMddHH").format(initDt),
                    ele
            );
            String metaFile = outDir + File.separator + outFile + "-meta.json";
            String binFile = outDir + File.separator + outFile + "-data.bin";

            //拼接参数，找到要读取数据文件的具体位置
            File indexFile = new File(metaFile);
            if (!indexFile.exists()) {
                return "fail";
            }
            String str = null;//前面两行是读取文件
            str = FileUtils.readFileToString(indexFile);
            JSONObject jsonobject = JSON.parseObject(str);
            RandomAccessFile raf = new RandomAccessFile(new File(binFile),"r");
            TreeMap rst = new TreeMap();
            for(String dataPath:jsonobject.keySet()){
                TreeMap<String,Float> dataPathRst = new TreeMap<>();
                rst.put(dataPath, dataPathRst);
                JSONObject object = jsonobject.getJSONObject(dataPath);
                for(String dataName: object.keySet()){
                    JSONObject obj = object.getJSONObject(dataName);
                    dataPathRst.put(dataName, CommonUtils.readPoint(raf, obj, Float.parseFloat(lon), Float.parseFloat(lat)));
                }
            }
            System.out.println(rst);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return "success";
    }


}
