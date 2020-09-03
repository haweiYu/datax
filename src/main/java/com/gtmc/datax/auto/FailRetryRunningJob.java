package com.gtmc.datax.auto;

import com.gtmc.datax.auto.cons.APPConstans;
import com.gtmc.datax.auto.util.DataJsonFormatter;
import com.gtmc.datax.auto.util.LinuxCMDUtils;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.Properties;

public class FailRetryRunningJob
{
    public static void main(String[] args)
            throws Exception
    {
        System.out.println("DataX### 正在读取您指定的" + args[0] + " 路径下的配置文件");
        Properties props = readTargetHiveTableProperties(args[0]);
        String projectName = props.getProperty("gtmc.project.name");
        String dataFilePath = props.getProperty("gtmc.datafile.path");
        String dbName = props.getProperty("gtmc.db.name");
        String tableName = props.getProperty("gtmc.table.name");
        String partitionColumn = props.getProperty("gtmc.partition.column.name");

        String dataxFilePath = APPConstans.BASE_DIR + "/" + projectName + "_" + tableName + "/datax.json";
        try
        {
            System.out.println("DataX### 重建" + tableName + "biao " + partitionColumn + "=" + args[1] + "分区");
            LinuxCMDUtils.executeHiveSQL("alter table " + dbName + "." + tableName + " drop if exists partition(" + partitionColumn + "='" + args[1] + "')");
            LinuxCMDUtils.executeHiveSQL("alter table " + dbName + "." + tableName + " add if not exists partition(" + partitionColumn + "='" + args[1] + "')");

            System.out.println("DataX### 正在读取DataX配置文件");
            DataJsonFormatter.changeEverydayPath(dataxFilePath, dbName, tableName, partitionColumn, dataFilePath, args[1]);
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        System.out.println("DataX### 开始执行DataX任务,数据抽取中");
        try
        {
            Runtime run = Runtime.getRuntime();
            Process process = run.exec(APPConstans.DATAX_PATH + " " + dataxFilePath);
            InputStream in = process.getInputStream();
            StringBuffer out = new StringBuffer();
            byte[] b = new byte[4096];
            int n;
            while ((n = in.read(b)) != -1) {
                out.append(new String(b, 0, n));
            }
            System.out.println("job result \n" + out.toString());
            if (out.toString().contains("DataXException")) {
                throw new Exception("DataX### DataX任务失败\n" + out.toString());
            }
            in.close();
            process.destroy();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    private static Properties readTargetHiveTableProperties(String PATH)
            throws IOException
    {
        Properties props = new Properties();
        InputStream in = new BufferedInputStream(new FileInputStream(PATH));
        props.load(in);
        return props;
    }
}
