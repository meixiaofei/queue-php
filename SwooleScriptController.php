<?php

namespace app\console\controllers;

use app\common\models\Course;
use yii\console\Controller;
use Yii;


class SwooleScriptController extends Controller
{

    /**
     * 启动直播课 直播状态监控进程 需要配合心跳处理进程 使用
     */
    public function actionMonitorLiveCourse()
    {
        \swoole_timer_tick(1000, function ($timer_id) {
            $yiiPath =  str_replace('/console','/',Yii::getAlias("@app"));
            exec("cd {$yiiPath} && APPLICATION_ENV=local php yii monitor/monitor-live-course");
        });
    }

    /**
     * 定时启动心跳处理进程
     */
    public function actionHeartbeat()
    {
        \swoole_timer_tick(1000, function ($timer_id) {
            $yiiPath =  str_replace('/console','/',Yii::getAlias("@app"));
            exec("cd {$yiiPath} && APPLICATION_ENV=local php yii monitor/heartbeat");
        });
    }

    /**
     * 定时分析人才库脚本
     * setsid php yii swoole-script/talent-analyse &
     */
    public function actionTalentAnalyse()
    {
        \swoole_timer_tick(60000, function ($timer_id) {
            $yiiPath =  str_replace('/console','/',Yii::getAlias("@app"));
            exec("cd {$yiiPath} && php yii talent/init");
        });
    }

    public function actionT()
    {
        echo '当前环境为'.YII_ENV."\n";
    }

}