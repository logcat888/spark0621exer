package com.atguigu.spark;

import java.util.Random;

/**
 * @author chenhuiup
 * @create 2020-09-23 15:50
 */
/*
1.创建Random对象，如果没有给初始值(seed)，就会默认生成一个以时间为单位的初始值，作为随机算法的初始值
2.种子就是随机算法的初始值，如果初始值相同生成的随机数也相同
 */
public class TestRandom {
    public static void main(String[] args) {
        Random r1 = new Random(10);

        for (int i = 0; i < 5; i++) {
            //生成10以内的整数
            System.out.println(r1.nextInt(10));
        }
        // 5  3  8 1 7
        System.out.println("--------------------");
        Random r2 = new Random(10);
        for (int i = 0; i < 5; i++) {
            //生成10以内的整数
            System.out.println(r2.nextInt(10));
        }
        // 5  3  8 1 7

        /*
        public Random() {
        this(seedUniquifier() ^ System.nanoTime());
    }
         */
    }
}
