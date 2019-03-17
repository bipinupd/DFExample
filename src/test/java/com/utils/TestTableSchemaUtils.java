package com.utils;

import com.google.api.services.bigquery.model.TableSchema;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;


public class TestTableSchemaUtils {

    @Test
    public void readSchemaTest() throws Exception{
        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(classLoader.getResource("gobikeschema.json").getFile());
        InputStream io = new FileInputStream(file);
        TableSchemaUtils utils = new TableSchemaUtils();
        TableSchema tc = utils.readSchema(io);
        System.out.print(tc.getFields().size());
        assertEquals(tc.getFields().size(), 13);
    }

}
