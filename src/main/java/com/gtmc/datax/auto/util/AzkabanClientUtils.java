package com.gtmc.datax.auto.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import java.io.PrintStream;

public class AzkabanClientUtils
{
    private static String AZKABAN_URL = "https://172.16.136.59:8443";

    public static String login()
    {
        String[] cmds = { "curl", "-k", "-X", "POST", "--data", "action=login&username=gtmc_dmp&password=admin", AZKABAN_URL };

        String loginResut = LinuxCMDUtils.execCurl(cmds);
        System.out.println(loginResut);
        if (loginResut.contains("success")) {
            return JSON.parseObject(loginResut).getString("session.id");
        }
        return null;
    }

    public static boolean createProject(String projectName, String tableName, String sessionId)
    {
        String taskJobName = projectName + "_" + tableName;
        String[] cmds = { "curl", "-k", "-X", "POST", "--data", "session.id=" + sessionId + "&name=" + taskJobName + "&description=" + taskJobName + "������", AZKABAN_URL + "/manager?action=create" };

        String createProjectResut = LinuxCMDUtils.execCurl(cmds);
        System.out.println(createProjectResut);
        return createProjectResut.contains("success");
    }

    public static void uploadZip(String taskZipPath, String projectName, String tableName, String sessionId)
    {
        String taskJobName = projectName + "_" + tableName;
        String[] cmds = { "curl", "-k", "-i", "-X", "POST", "--form", "session.id=" + sessionId, "--form", "ajax=upload", "--form", "file=@" + taskZipPath + ";type=application/zip", "--form", "project=" + taskJobName, AZKABAN_URL + "/manager" };

        String uploadZipResut = LinuxCMDUtils.execCurl(cmds);
        System.out.println(uploadZipResut);
    }

    public static void scheduleTask(String sessionId, String projectName, String tableName, String startDate, String scheduleHour, String scheduleMinu, String amOrpm)
    {
        String taskJobName = projectName + "_" + tableName;
        String[] getTaskInfoCMDs = { "curl", "-k", "--get", "--data", "session.id=" + sessionId + "&ajax=fetchprojectflows&project=" + taskJobName, AZKABAN_URL + "/manager" };

        String resultJson = LinuxCMDUtils.execCurl(getTaskInfoCMDs);
        String projectId = JSON.parseObject(resultJson).getString("projectId");

        String[] scheduleCMDs = { "curl", "-k", AZKABAN_URL + "/schedule", "-d", "ajax=scheduleFlow&is_recurring=on&period=1d&projectName=" + projectName + "_" + tableName + "&flow=first&projectId=" + projectId + "&scheduleTime=" + scheduleHour + "," + scheduleMinu + "," + amOrpm + ",PDT&scheduleDate=" + startDate, "-b", "azkaban.browser.session.id=" + sessionId, AZKABAN_URL + "/manager" };

        String scheduleResult = LinuxCMDUtils.execCurl(scheduleCMDs);
        System.out.println(scheduleResult);
    }
}

