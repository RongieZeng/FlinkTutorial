package com.atguigu.chapter11.source;

/**
 * Copyright (c) 2020-2030 尚硅谷 All Rights Reserved
 * <p>
 * Project:  FlinkTutorial
 * <p>
 * Created by  wushengran
 */

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class AutodialerNumberSource implements SourceFunction<AutodialerNumberDetail> {
    // 声明一个布尔变量，作为控制数据生成的标识位
    private Boolean running = true;
    @Override
    public void run(SourceContext<AutodialerNumberDetail> ctx) throws Exception {
        Thread.sleep(1000*30);
        Random random = new Random();    // 在指定的数据集中随机选取数据

            for (int i = 0; i < 1000; i++) {
                if(!running){
                    break;
                }

                AutodialerNumberDetail autodialerNumberDetail = new AutodialerNumberDetail();
                autodialerNumberDetail.taskId = random.nextInt(2);
                autodialerNumberDetail.numberId = random.nextInt(100);
                Thread.sleep(1000);

                ctx.collect(autodialerNumberDetail);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }

}
