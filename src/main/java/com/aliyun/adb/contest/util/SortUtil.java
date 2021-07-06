package com.aliyun.adb.contest.util;

/**
 * @author hum
 */
public class SortUtil {
    // max kth
    public static long quickSelect(long[] nums, int k, int start, int end) {
        while (true) {
            if (start == end) {
                return nums[start];
            }
            int left = start;
            int right = end;
            long pivot = nums[(start + end) >> 1];
            while (left <= right) {
                while (left <= right && nums[left] < pivot) {
                    left++;
                }
                while (left <= right && nums[right] > pivot) {
                    right--;
                }
                if (left <= right) {
                    long temp = nums[left];
                    nums[left] = nums[right];
                    nums[right] = temp;
                    left++;
                    right--;
                }
            }
            if (start + k - 1 <= right) {
                end = right;
//                return quickSelect(nums, k, start, right);
            } else if (start + k - 1 >= left) {
                k = k - (left - start);
                start = left;
//                return quickSelect(nums, k - (left - start), left, end);
            } else {
                return nums[right + 1];
            }
        }

    }

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
