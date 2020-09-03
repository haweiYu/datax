package com.gtmc.datax.auto;

import com.gtmc.datax.auto.cons.APPConstans;
import com.gtmc.datax.auto.util.AzkabanClientUtils;
import com.gtmc.datax.auto.util.DataJsonFormatter;
import com.gtmc.datax.auto.util.LinuxCMDUtils;
import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;

public class Deployment
{
    public static void main(String[] args)
    {
        if ((args[0] != null) && (args[0].endsWith(".properties")))
        {
            extractOneTable(args);
        }
        else
        {
            File[] tmpFilePath = new File(args[0]).listFiles();
            ArrayList<String> resultFilePath = new ArrayList();
            for (File filePath : tmpFilePath) {
                if (filePath.getAbsolutePath().endsWith(".properties")) {
                    resultFilePath.add(filePath.getAbsolutePath());
                }
            }
            Iterator<String> iterator = resultFilePath.iterator();
            while (iterator.hasNext()){
                String filePath = iterator.next();
                extractOneTable(new String[] { filePath });
            }
        }
    }

    public static void extractOneTable(String[] args)
    {
        try
        {
            System.out.println("DataX### 正在读取您指定路径下的文件" + args[0] + " 路径下的配置文件");
            Properties props = readTargetHiveTableProperties(args[0]);
            String projectName = props.getProperty("gtmc.project.name");
            String dataFilePath = props.getProperty("gtmc.datafile.path");
            String skipFileHeader = props.getProperty("gtmc.datafile.skipHeader");
            String fileDelimited = props.getProperty("gtmc.datafile.delimited");
            String dbName = props.getProperty("gtmc.db.name");
            String tableName = props.getProperty("gtmc.table.name");
            String tableDelimited = props.getProperty("gtmc.table.delimited");
            String[] columns = props.getProperty("gtmc.columns.name").split("\\|");
            String[] columnTypes = props.getProperty("gtmc.columns.type").split("\\|");
            String partitionColumn = props.getProperty("gtmc.partition.column.name");
            String startDate = props.getProperty("gtmc.schedule.start.date");
            String scheduleHour = props.getProperty("gtmc.schedule.every.hour");
            String scheduleMinu = props.getProperty("gtmc.schedule.every.minu");
            String amOrpm = props.getProperty("gtmc.schedule.every.amOrpm");

            System.out.println("DataX### 正在构建项目目录");
            String projectDirPath = APPConstans.BASE_DIR + "/" + projectName + "_" + tableName;
            LinuxCMDUtils.executeLocal("mkdir " + projectDirPath);
            LinuxCMDUtils.executeLocal("cp " + args[0] + " " + projectDirPath);

            System.out.println("DataX### 正在为项目" + projectName + "构建库表 " + tableName + "...");
            String columnsSQL = properties2ColumnsSQL(columns, columnTypes);
            String CREATE_TABLE_SQL = "create table if not exists " + dbName + "." + tableName + columnsSQL + " partitioned by (" + partitionColumn + " string) row format delimited fields terminated by '\\t'";
            LinuxCMDUtils.executeHiveSQL(CREATE_TABLE_SQL);

            System.out.println("DataX### 正在读取DataX.json配置文件");
            DataJsonFormatter.createProjectedTemplate(APPConstans.BASE_DIR + "/common/demo.json", projectDirPath + "/datax.json", dataFilePath,
                    Boolean.parseBoolean(skipFileHeader), columns.length, fileDelimited, "", columns, columnTypes, partitionColumn, tableDelimited);

            System.out.println("DataX### 正在打包Azkaban依赖文件");
            createAzkabanJobFile(projectDirPath, projectName, dbName, tableName, partitionColumn, dataFilePath);
            String taskZipPath = projectDirPath + "/task.zip";
            String azkabanJobFilePath = projectDirPath + "/first.job";
            String jarFilePath = APPConstans.BASE_DIR + "/common/DataXAutoDeployment-1.0-SNAPSHOT.jar";
            LinuxCMDUtils.executeLocal("zip -pj " + taskZipPath + " " + jarFilePath + " " + azkabanJobFilePath);

            System.out.println("DataX### 正在执行Azkaban任务链");
            System.out.println("DataX### 自动登录中");
            String azkabanSessonId = AzkabanClientUtils.login();
            if (azkabanSessonId == null) {
                throw new NullPointerException("DataX### Azkaban登陆失败!!!");
            }
            System.out.println("DataX### 创建当前入户表的的任务");
            boolean isSuccessCreateProject = AzkabanClientUtils.createProject(projectName, tableName, azkabanSessonId);
            if (!isSuccessCreateProject) {
                throw new NullPointerException("DataX### Azkaban任务创建失败");
            }
            System.out.println("DataX### 尝试上传zip压缩包");
            AzkabanClientUtils.uploadZip(taskZipPath, projectName, tableName, azkabanSessonId);
            System.out.println("DataX### 定时调度跑一个任务");
            AzkabanClientUtils.scheduleTask(azkabanSessonId, projectName, tableName, startDate, scheduleHour, scheduleMinu, amOrpm);
        }
        catch (Exception e)
        {
            System.out.println("DataX### 程序报错: " + e.getLocalizedMessage());
        }
    }

    private static void createAzkabanJobFile(String projectDirPath, String projectName, String dbName, String tableName, String partitionColumn, String dataFilePath)
    {
        try
        {
            BufferedWriter writer = new BufferedWriter(new FileWriter(projectDirPath + "/first.job"));
            writer.write("# first.job\n");
            writer.write("type=command\n");
            writer.write("command=java -classpath DataXAutoDeployment-1.0-SNAPSHOT.jar com.gtmc.datax.auto.AutoExecute " + projectName + " " + dbName + " " + tableName + " " + partitionColumn + " " + dataFilePath);
            writer.close();
        }
        catch (Exception e)
        {
            System.out.println("DataX### Azkaban Job文件配置失败:" + e.getLocalizedMessage());
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

    private static String properties2ColumnsSQL(String[] columns, String[] columnTypes)
    {
        String[] resultArr = new String[columns.length];
        if (columns.length != columnTypes.length) {
            throw new IllegalArgumentException("DataX### 字段长度不匹配");
        }
        for (int i = 0; i < columns.length; i++) {
            resultArr[i] = (columns[i] + " " + columnTypes[i]);
        }
        return Arrays.toString(resultArr).replace("[", "(").replace("]", ")");
    }
}
