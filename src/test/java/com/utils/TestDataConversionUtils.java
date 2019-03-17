package com.utils;

import org.junit.Test;

import javax.swing.plaf.synth.SynthEditorPaneUI;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class TestDataConversionUtils {

    @Test
    public void dateConversionTest() throws Exception{
        String date = "2019-01-30 17:46:10.8460";
        Date dt = com.utils.DataConversionUtils.getDateFromString("yyyy-MM-dd HH:mm:ss.SSSS", date);
        assertNotNull(dt);
//        assertEquals(Double.parseDouble("1548339152900"), dt.getTime());
    }
}
