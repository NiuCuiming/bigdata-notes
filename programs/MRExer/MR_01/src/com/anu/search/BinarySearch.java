package com.anu.search;

public class BinarySearch {


    public static void main(String[] args) {

        System.out.println("Start...");
        int[] myArray = new int[] { 1, 2, 3, 5, 6, 7, 8, 9 };
        System.out.println("查找数字8的下标：");
        System.out.println(binarySearch(myArray, 8));
    }

    //二分查找
    public static int binarySearch(int[] arrs,int number) {

        /*
        查找的数组要求有序
        1. 定义左右边界
        2. 计算中间位置
        3.对比
        4.返回或者重新定义左右边界
         */

        int begin = 0;
        int end = arrs.length-1;
        int mid = -1;

        while(end > begin) {

            mid = (begin+end)/2;
            if(arrs[mid] == number) {
                return mid;
            } else if(arrs[mid] > number) {
                end = mid -1;
            } else if (arrs[mid] < number) {
                begin = mid +1;
            }
        }


        return -1;


    }


}
