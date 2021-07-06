package com.aliyun.adb.contest.util;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author hum
 * @date 2021/7/6
 */
public class SortUtilTest {

    @Test
    public void findKthLargest() {
        long[] nums = new long[]{2, 4, 7, 123, 623, 456};
        System.out.println(SortUtil.findKthLargest(nums, 4, 0, nums.length));
        long[] nums2 = new long[]{4, 8, 3, 1, 2, 5, 7, 6};
        System.out.println(SortUtil.quickSelect(nums2, 1, 0, nums2.length - 1));
    }
}