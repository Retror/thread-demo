package com.moap.dataprocess.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.hyt.common.LatLng;
import com.hyt.common.LatLngBounds;
import com.hyt.common.NormalData;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.TreeMap;

public class CommonUtils {
    /**
     * float数组转换byte数组
     *
     * @param values
     * @return
     */
    public static byte[] floatArrayToByteArray(float[] values) {
        ByteBuffer buffer = ByteBuffer.allocate(4 * values.length);
        for (float value : values) {
            buffer.putFloat(value);
        }
        return buffer.array();
    }


    public static ByteBuffer floatArrayToByteBuffer(float[] values) {
        ByteBuffer buffer = ByteBuffer.allocate(4 * values.length);
        for (float value : values) {
            buffer.putFloat(value);
        }
        return buffer;
    }

    /**
     * normaldata转换jsonobject
     *
     * @param normalData
     * @return
     */
    public static JSONObject normalData2JsonObject(NormalData normalData,int index) {
        float[][] data = normalData.data;
        //数据总行数
        int y = data.length;
        //数据总列数
        int x = data[0].length;
        //计算数据位置
        long position = CommonUtils.getPosition(index, x, y);
        LatLngBounds bounds = normalData.bounds;
        LatLng northEast = bounds._northEast;
        LatLng southWest = bounds._southWest;
        //组装最里面的数据内容
        JSONObject content = new JSONObject();
//        content.put("position", 0);
        content.put("position", position);
        content.put("Y", y);
        content.put("X", x);
        content.put("startLat", southWest.lat);
        content.put("endLat", northEast.lat);
        content.put("startLon", southWest.lng);
        content.put("endLon", northEast.lng);
        content.put("res", normalData.res);
        return content;
    }

    /**
     * 获取位置
     *
     * @param index 数据文件索引
     * @param x     数据列
     * @param y     数据行
     * @return
     */
    public static long getPosition(int index, int x, int y) {
        long position = index * x * y * 4;
        return position;
    }

    /**
     * 读取单个点
     * @param raf
     * @param jsonObject
     * @param lon
     * @param lat
     * @return
     * @throws IOException
     */
    public static Float readPoint(RandomAccessFile raf, JSONObject jsonObject, Float lon, Float lat) throws IOException{
        float res = jsonObject.getFloatValue("res");
        long position = jsonObject.getLongValue("position");
        float startLon = jsonObject.getFloatValue("startLon");
        float startLat = jsonObject.getFloatValue("startLat");
        int X = jsonObject.getIntValue("X");
        int x = Math.round((lon - startLon)/res);
        int y = Math.round((lat - startLat)/res);
        raf.seek(position+ y*X + x);
        return raf.readFloat();
    }

}
