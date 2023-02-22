package com.moap.dataprocess.util;

import com.hyt.common.LatLngBounds;
import com.hyt.common.NormalData;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.*;
import java.net.URI;

public class TestUtil {
    public static void main(String[] args) throws IOException, ClassNotFoundException {
//        NormalData normalData = new NormalData();
//        normalData.res=0.2f;
//        normalData.bounds=new LatLngBounds();
//        normalData.data=new float[][]{{0.1f,0.2f},{0.2f,0.3f}};

//        String url = "http://10.1.64.146/mdfs/v1.1/data?dataPath=ECMWF_HR/HGT/500/23021508.240";
//        HttpClient httpClient = HttpClients.createDefault();
//        HttpGet httpGet = new HttpGet();
//        //设置请求和传输超时时间
//        RequestConfig requestConfig = RequestConfig.custom().setSocketTimeout(60000).setConnectTimeout(60000).build();
//        httpGet.setConfig(requestConfig);
//        httpGet.setURI(URI.create(url));
//        HttpResponse response = httpClient.execute(httpGet);
//
//        byte[] bytes = EntityUtils.toByteArray(response.getEntity());
//        System.out.println(new String(bytes));

//        ByteArrayOutputStream byt=new ByteArrayOutputStream();
//        ObjectOutputStream obj=new ObjectOutputStream(byt);
//        obj.writeObject(normalData);
//        byte[] byteArray=byt.toByteArray();




//        NormalData normalData1 = new NormalData();
//        if(byteArray!=null) {
//            ByteArrayInputStream byteInt = new ByteArrayInputStream(byteArray);
//            ObjectInputStream objInt = new ObjectInputStream(byteInt);
//            normalData1 = (NormalData) objInt.readObject();
//        }
//        System.out.println(normalData1.res+"-"+normalData1.bounds+"-"+normalData1.data[0][0]);


    }
}
