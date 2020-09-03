package com.gtmc.datax.auto.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

public class LinuxCMDUtils
{
    public static void executeLocal(String cmd)
    {
        System.out.println("cmd job : " + cmd);
        Runtime run = Runtime.getRuntime();
        try
        {
            Process process = run.exec(cmd);
            InputStream in = process.getInputStream();
            StringBuffer out = new StringBuffer();
            byte[] b = new byte[4096];
            int n;
            while ((n = in.read(b)) != -1) {
                out.append(new String(b, 0, n));
            }
            System.out.println("job result \n" + out.toString());
            in.close();
            process.destroy();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    public static void executeHiveSQL(String sql)
    {
        try
        {
            System.out.println("hive sql job : " + sql);
            List<String> command = new ArrayList();
            command.add("hive");
            command.add("-e");
            command.add(sql);
            ProcessBuilder hiveProcessBuilder = new ProcessBuilder(command);
            Process hiveProcess1 = hiveProcessBuilder.start();
            System.out.println("DataX### 执行SQL" + (hiveProcess1.waitFor() == 0 ? "成功" : "失败") + "... ");
        }
        catch (Exception e)
        {
            System.out.println("DataX### 执行SQL失败" + e.getLocalizedMessage());
        }
    }

    public static String execCurl(String[] cmds)
    {
        ProcessBuilder process = new ProcessBuilder(cmds);
        try
        {
            Process p = process.start();
            BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
            StringBuilder builder = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null)
            {
                builder.append(line);
                builder.append(System.getProperty("line.separator"));
            }
            return builder.toString();
        }
        catch (IOException e)
        {
            System.out.print("error");
            e.printStackTrace();
        }
        return null;
    }
}
