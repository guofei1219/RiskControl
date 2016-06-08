package io;

import org.junit.Test;

import java.io.*;

/**
 * Created by 郭飞 on 2016/6/7.
 */
public class BufferDemo {
    public void StringBufferDemo() throws IOException {
        File file=new File("/root/sms.log");
        if(!file.exists())
            file.createNewFile();
        FileOutputStream out=new FileOutputStream(file,true);
        for(int i=0;i<10000;i++){
            StringBuffer sb=new StringBuffer();
            sb.append("这是第"+i+"行:前面介绍的各种方法都不关用,为什么总是奇怪的问题 ");
            out.write(sb.toString().getBytes("utf-8"));
        }
        out.close();
    }

    /**
     * 多次申请核查
     * @throws IOException
     */
    @Test
    public void DuoCiShenQing_BufferedReader() throws IOException {
        String path="C://work//ideabench//RiskControl//src//main//resources//file//bairong_DuoCiShenCha";
        File file=new File(path);

        String path_result="C://work//ideabench//RiskControl//src//main//resources//file//bairong_DuoCiShenCharesult.txt";
        File file_result = new File(path_result);

        if(!file.exists()||file.isDirectory())
            throw new FileNotFoundException();

        BufferedReader br=new BufferedReader(new FileReader(file));
        BufferedWriter bw = new BufferedWriter(new FileWriter(path_result));

        String line;

        while((line = br.readLine())!=null){

            StringBuffer sb=new StringBuffer();
            sb.append("\"");
            //temp=br.readLine();
            String collum = line.split("\t")[0];
            if(collum.contains("selfnum")){
                sb.append(collum+"\":\"1\"");
            }else if(collum.contains("orgnum")) {
                sb.append(collum+"\":\"2\"");
            }else if(collum.contains("allnum")){
                sb.append(collum+"\":\"3\"");
            }else{
                sb.append(collum+"\":\"4\"");
            }

            bw.write(sb.toString());
            bw.write("\n");
            bw.flush();
            System.out.print(sb.toString()+"\n");
            sb = null;
        }
        //return sb.toString();

    }


    /**
     * 特殊名单核查
     * @throws IOException
     */
    @Test
    public void TeShuMingDan_BufferedReader() throws IOException {
        String path="C://work//ideabench//RiskControl//src//main//resources//file//bairong.txt";
        File file=new File(path);

        String path_result="C://work//ideabench//RiskControl//src//main//resources//file//result2.txt";
        File file_result = new File(path_result);

        if(!file.exists()||file.isDirectory())
            throw new FileNotFoundException();

        BufferedReader br=new BufferedReader(new FileReader(file));
        BufferedWriter bw = new BufferedWriter(new FileWriter(path_result));

        String line;

        while((line = br.readLine())!=null){

            StringBuffer sb=new StringBuffer();
            sb.append("\"");
            //temp=br.readLine();
            String collum = line.split("\t")[0];
            if(collum.contains("ovdue")){
                sb.append(collum+"\":\"0\"");
            }else {
                sb.append(collum+"\":\"1\"");
            }

            bw.write(sb.toString());
            bw.write("\n");
            bw.flush();
            System.out.print(sb.toString()+"\n");
            sb = null;
        }
        //return sb.toString();

    }

    /**
     * 拼接Json key
     * @throws IOException
     */
    @Test
    public void Concat_Conf() throws IOException {
        String path="C://work//ideabench//RiskControl//src//main//resources//file//bairong.txt";
        String path1="C://work//ideabench//RiskControl//src//main//resources//file//bairong_DuoCiShenCha";
        File file=new File(path);

        String path_result="C://work//ideabench//RiskControl//src//main//resources//file//collum_conf.txt";
        File file_result = new File(path_result);

        if(!file.exists()||file.isDirectory())
            throw new FileNotFoundException();

        BufferedReader br=new BufferedReader(new FileReader(file));
        //BufferedWriter bw = new BufferedWriter(new FileWriter(path_result));

        String line;
        StringBuffer sb=new StringBuffer();
        sb.append("\"");
        while((line = br.readLine())!=null){
            String collum = line.split("\t")[0];
            sb.append(collum+"\t");
        }
        sb.append("\"");
        System.out.print(sb.toString()+"\n");
    }
}
