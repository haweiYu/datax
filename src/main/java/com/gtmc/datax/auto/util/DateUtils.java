package com.gtmc.datax.auto.util;

import java.text.SimpleDateFormat;
import java.util.Date;

public class DateUtils
{
    public static String currentFormatDate()
    {
        SimpleDateFormat df = new SimpleDateFormat("YYYY-MM-dd");
        return df.format(new Date());
    }
}

