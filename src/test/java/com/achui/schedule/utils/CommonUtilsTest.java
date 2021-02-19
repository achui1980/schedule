package com.achui.schedule.utils;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class CommonUtilsTest {

    @Test
    void formatSize() {
        int result = CommonUtils.formatSize(32);
        assertEquals(32, result);
    }
}