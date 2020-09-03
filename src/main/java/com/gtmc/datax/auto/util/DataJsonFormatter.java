package com.gtmc.datax.auto.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;

public class DataJsonFormatter
{
    public static void createProjectedTemplate(String templateJsonFilePath, String jsonFilePath, String dataFilePath, boolean skipHeader, int colsSize, String fileDelimited, String partitionFilePath, String[] columns, String[] columnTypes, String partitionColumn, String tableDlimited)
            throws IOException
    {
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(templateJsonFilePath)));
        StringBuilder jsonSB = new StringBuilder();
        String lineBuff = null;
        while ((lineBuff = reader.readLine()) != null) {
            jsonSB.append(lineBuff);
        }
        JSONObject dataXJson = JSONObject.parseObject(jsonSB.toString());

        JSONObject readerJsonObject = dataXJson.getJSONObject("job").getJSONArray("content").getJSONObject(0).getJSONObject("reader");
        JSONArray dataFilePathArr = new JSONArray();
        dataFilePathArr.add(dataFilePath);
        readerJsonObject.getJSONObject("parameter").put("path", dataFilePathArr);
        readerJsonObject.getJSONObject("parameter").put("skipHeader", Boolean.valueOf(skipHeader));
        readerJsonObject.getJSONObject("parameter").put("column", fileColsFormateJson(colsSize, columnTypes));
        readerJsonObject.getJSONObject("parameter").put("fieldDelimiter", fileDelimited);

        JSONObject writerJsonObject = dataXJson.getJSONObject("job").getJSONArray("content").getJSONObject(0).getJSONObject("writer");
        writerJsonObject.getJSONObject("parameter").put("path", partitionFilePath);
        writerJsonObject.getJSONObject("parameter").put("fieldDelimiter", tableDlimited);
        JSONArray columnJsonArr = new JSONArray();
        for (int i = 0; i < columns.length; i++) {
            columnJsonArr.add(JSON.parseObject("{\"name\": \"" + columns[i] + "\",\"type\": \"" + columnTypes[i] + "\"}"));
        }
        columnJsonArr.add(JSON.parseObject("{\"name\": \"" + partitionColumn + "\",\"type\": \"string\"}"));
        writerJsonObject.getJSONObject("parameter").put("column", columnJsonArr);

        BufferedWriter writer = new BufferedWriter(new FileWriter(jsonFilePath));
        writer.write(dataXJson.toString());
        writer.close();
    }

    private static JSONArray fileColsFormateJson(int colsSize, String[] columnTypes)
    {
        JSONArray jsonArr = new JSONArray();
        for (int i = 0; i < colsSize; i++) {
            jsonArr.add(JSON.parseObject("{\"index\": " + i + ",\"type\": \"" + TypeMatcher.hiveType2FileColType(columnTypes[i]) + "\"}"));
        }
        return jsonArr;
    }

    public static void changeEverydayPath(String dataxFilePath, String dbName, String tableName, String partitionName, String dataFilePath, String extractTime)
            throws IOException
    {
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(dataxFilePath)));
        StringBuilder jsonSB = new StringBuilder();
        String lineBuff = null;
        while ((lineBuff = reader.readLine()) != null) {
            jsonSB.append(lineBuff);
        }
        JSONObject dataXJson = JSONObject.parseObject(jsonSB.toString());

        JSONObject readerJsonObject = dataXJson.getJSONObject("job").getJSONArray("content").getJSONObject(0).getJSONObject("reader");
        JSONArray dataFilePathArr = new JSONArray();
        dataFilePathArr.add(dataFilePath + "/" + (extractTime == null ? DateUtils.currentFormatDate() : extractTime) + "/*");
        readerJsonObject.getJSONObject("parameter").put("path", dataFilePathArr);

        JSONObject writerJsonObject = dataXJson.getJSONObject("job").getJSONArray("content").getJSONObject(0).getJSONObject("writer");
        writerJsonObject.getJSONObject("parameter").put("path", "/user/hive/warehouse/" + dbName + ".db/" + tableName
                .toLowerCase() + "/" + partitionName.toLowerCase() + "=" + DateUtils.currentFormatDate());
        BufferedWriter writer = new BufferedWriter(new FileWriter(dataxFilePath));
        writer.write(dataXJson.toString());
        writer.close();
    }
}

