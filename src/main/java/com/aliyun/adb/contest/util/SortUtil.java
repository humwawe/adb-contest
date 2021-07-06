package com.aliyun.adb.contest.util;

/**
 * @author hum
 */
public class SortUtil {
    public static long findKthLargest(long[] nums, int k, int l, int r) {
        int left = l;
        int right = r - 1;
        while (true) {
            assert left <= right;
            int index = partition(nums, left, right);
            if (index == k + l) {
                return nums[index];
            } else if (index < k + l) {
                left = index + 1;
            } else {
                right = index - 1;
            }
        }
    }

    public static long findKthLargest(long[] nums, int k) {
        return findKthLargest(nums, k, 0, nums.length);
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
