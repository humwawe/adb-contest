package com.aliyun.adb.contest.util;

/**
 * @author hum
 */
public class SortUtil {
    public static long findKthLargest(long[] nums, int k) {
        int len = nums.length;
        int left = 0;
        int right = len - 1;
        while (true) {
            int index = partition(nums, left, right);
            if (index == k) {
                return nums[index];
            } else if (index < k) {
                left = index + 1;
            } else {
                right = index - 1;
            }
        }
    }

    private static int partition(long[] nums, int left, int right) {
        long pivot = nums[left];
        int j = left;
        for (int i = left + 1; i <= right; i++) {
            if (nums[i] < pivot) {
                j++;
                swap(nums, j, i);
            }
        }
        swap(nums, j, left);
        return j;
    }

    private static void swap(long[] nums, int index1, int index2) {
        long temp = nums[index1];
        nums[index1] = nums[index2];
        nums[index2] = temp;
    }

}
