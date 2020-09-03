package com.gtmc.datax.auto;

import com.gtmc.datax.auto.cons.APPConstans;
import com.gtmc.datax.auto.util.DataJsonFormatter;
import com.gtmc.datax.auto.util.DateUtils;
import com.gtmc.datax.auto.util.LinuxCMDUtils;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;

public class AutoExecute
{
    public static void main(String[] args)
            throws Exception
    {
        String dataxFilePath = APPConstans.BASE_DIR + "/" + args[0] + "_" + args[2] + "/datax.json";
        try
        {
            System.out.println("DataX### 正在读取DataX配置文件");
            DataJsonFormatter.changeEverydayPath(dataxFilePath, args[1], args[2], args[3], args[4], null);

            LinuxCMDUtils.executeHiveSQL("alter table " + args[1] + "." + args[2] + " add if not exists partition(" + args[3] + "='" + DateUtils.currentFormatDate() + "')");
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
}

