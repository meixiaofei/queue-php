<?php


namespace app\console\controllers;

use app\manager\models\AuthAdmin;
use app\module\notification\models\Message;
use app\module\notification\models\MessageUserRelation;

class MessageQueueController extends BaseController
{
    // 待处理队列
    static $sendingTask = 'message:sending';
    // swoole队列
    static $swooleTask = 'message:swoole';
    // 失败队列 
    static $failTask = 'message:fail';
    // 任务队列
    static $missionTask = 'message:mission';
    // 定时队列
    static $cronTask = 'message:cron';
    // redis key prefix
    static $redisKeyPrefix = 'message:prefix';
    // message path prefix
    static $pathPrefix = 'message:path';
    // fd prefix
    static $fdPrefix = 'message:fd';
    // fd:path prefix
    static $fdPathPrefix = 'message:fd:path';
    // fd:token prefix
    static $fdTokenPrefix = 'message:fd:token';
    // redis 缓存存活时间
    static $redisExpireTime = 7200;
    // plug token prefix
    static $plugTokenPrefix = 'plug-';
    
    // 最大进程数量
    private $maxProcesses = 100;
    // 当前进程数量
    private $child = 0;
    
    /**
     * 子进程处理逻辑
     *
     * @param \swoole_process $worker
     */
    public function process(\swoole_process $worker)
    {
        \swoole_event_add($worker->pipe, function ($pipe) use ($worker) {
            // $buffer_size 是缓冲区的大小 默认为8192 最大不超过64K
            $workerData    = $worker->read(1024000);
            $workerDataArr = json_decode($workerData, true);
            switch ($workerDataArr['worker_type']) {
                case 'eat-sending':
                    $this->eatMessage($workerDataArr);
                    break;
                case 'eat-cron':
                    $this->eatMessage($workerDataArr);
                    break;
            }
            $worker->exit(0);
        });
    }
    
    public function actionHelp()
    {
        $commands = [
            'message-queue/help',
            'message-queue/fork-swoole',
            'message-queue/eat-sending',
            'message-queue/eat-cron',
        ];
        
        foreach ($commands as $cmd) {
            list(, $actionName) = explode('/', $cmd);
            if ($actionName != 'help') {
                echo "    nohup php yii {$cmd} >> nohup-{$actionName}.txt 2>&1 &\n";
            } else {
                echo "    php yii " . $cmd . "\n";
            }
        }
        echo "\n";
    }
    
    /**SUNLANDS
     *
     * nohup php yii message-queue/fork-swoole >> nohup-swoole.txt 2>&1 &
     *
     * @url          /notification-ws
     * @schemes      ws
     * @method get
     * @tags         消息模块
     * @description  主动心跳事件,传输数据用json,包含msgType(消息类型):heartbeat
     * @summary      消息socket接口
     *
     * @param string token / query 渠道id true
     * @param string message_path_id 3 query 渠道id(cronus为3,chrome为8) true
     *
     * @return string 获取成功
     */
    public function actionForkSwoole()
    {
        //创建websocket服务器对象，监听0.0.0.0:9502端口
        $ws = new \swoole_websocket_server('0.0.0.0', 9502);
        
        //监听WebSocket连接打开事件
        $ws->on('open', function (\swoole_websocket_server $ws, \swoole_http_request $request) {
            $token = isset($request->get['token']) ? $request->get['token'] : null;
            // 插件的token默认有效期一个月
            $trimToken   = str_replace(self::$plugTokenPrefix, '', $token);
            $isPlugToken = false;
            if (false !== strpos($token, self::$plugTokenPrefix)) {
                $isPlugToken = true;
            }
            if ((false === $user = $this->redis()->get($token)) && false === $user = $this->redis()->get($trimToken)) {
                $out = $this->asJson(FAIL, 'invalidate token');
            } else {
                if ($isPlugToken) {
                    $this->redis()->set($token, $user, 2592000);
                }
                $username           = json_decode($user, true)['employeeId263'];
                $messagePathId      = isset($request->get['message_path_id']) ? $request->get['message_path_id'] : 3;
                $pathPrefixUsername = self::$pathPrefix . $username;
                $this->redis()->set(self::$fdTokenPrefix . $request->fd, $token, self::$redisExpireTime);
                $this->redis()->set(self::$fdPathPrefix . $request->fd, $messagePathId, self::$redisExpireTime);
                $this->redis()->sadd($pathPrefixUsername . $messagePathId, $request->fd);
                $this->redis()->expire($pathPrefixUsername . $messagePathId, self::$redisExpireTime);
                $this->redis()->set(self::$fdPrefix . $request->fd, $username, self::$redisExpireTime);
                $out = $this->asJson(SUCCESS, 'socket握手成功' . $request->fd . $username);
            }
            $ws->push($request->fd, $out);
        });
        
        //监听WebSocket消息事件
        $ws->on('message', function (\swoole_websocket_server $ws, \swoole_websocket_frame $frame) {
            $message = json_decode($frame->data, true);
            if (isset($message['msgType'])) {
                switch ($message['msgType']) {
                    case 'heartbeat':
                        if (false !== $username = $this->redis()->get(self::$fdPrefix . $frame->fd)) {
                            $token = $this->redis()->get(self::$fdTokenPrefix . $frame->fd);
                            $this->redis()->expire(self::$fdTokenPrefix . $frame->fd, self::$redisExpireTime);
                            if (false !== strpos($token, self::$plugTokenPrefix)) {
                                $expire = 2592000;
                                $this->redis()->expire($token, $expire);
                            } else {
                                $expire = self::$redisExpireTime;
                            }
                            $this->redis()->expire(self::$fdPrefix . $frame->fd, self::$redisExpireTime);
                            $messagePathId = $this->redis()->get(self::$fdPathPrefix . $frame->fd);
                            $this->redis()->expire(self::$fdPathPrefix . $frame->fd, self::$redisExpireTime);
                            $this->redis()->expire(self::$pathPrefix . $username . $messagePathId, self::$redisExpireTime);
                            $ws->push($frame->fd, $this->asJson(SUCCESS, '心跳成功', $frame->fd . $username));
                        } else {
                            $ws->push($frame->fd, $this->asJson(FAIL, '非法心跳'));
                        }
                        break;
                    default:
                        $ws->push($frame->fd, $this->asJson(FAIL, 'unknown msgType'));
                }
            } else {
                $ws->push($frame->fd, $this->asJson(FAIL, '需以json字符串包含消息类型msgType:heartbeat'));
            }
        });
        
        $ws->on('WorkerStart', function (\swoole_websocket_server $server, $workerId) {
//            mll("worker started: {$workerId}");
            if ($workerId == 0) {
                // 定时主动推送数据
                $server->tick(1000, function () use ($server) {
                    while ($dataPop = $this->redis()->rpop(self::$swooleTask)) {
                        // ['message_id', 'sender_uid', 'receiver_uid', 'message_path_id', 'is_todo', 'is_red_flag', 'username'];
                        $dataArr       = json_decode($dataPop, true);
                        $username      = $dataArr['username'];
                        $messagePathId = $dataArr['message_path_id'];
                        $pup           = self::$pathPrefix . $username . $messagePathId;
                        foreach ($this->redis()->smembers($pup) as $connectedFd) {
                            if ($server->exist($connectedFd)) {
                                $pushData = Message::getListData($dataArr, $dataArr['receiver_uid']);
                                $server->push($connectedFd, $this->asJson(SUCCESS, '', $pushData['lists']));
                            } else {
                                $this->redis()->srem($connectedFd, $pup);
                            }
                        }
                    }
                });
            }
        });
        
        //监听WebSocket连接关闭事件
        $ws->on('close', function (\swoole_websocket_server $ws, $fd) {
            $username      = $this->redis()->get(self::$fdPrefix . $fd);
            $messagePathId = $this->redis()->get(self::$fdPathPrefix . $fd);
            if ($this->redis()->srem($fd, self::$pathPrefix . $username . $messagePathId) && $this->redis()->del(self::$fdPrefix . $fd)) {
                $ws->push($fd, $this->asJson(SUCCESS, 'socket握手断开成功'));
            }
        });
        
        $ws->start();
    }
    
    /**
     * signalHandler
     * 将当前fork起的进程数量进行回收
     *
     * @param $signal
     */
    private function signalHandler($signal)
    {
        switch ($signal) {
            case SIGCHLD:
                while ($ret = \swoole_process::wait(false)) {
                    /*$pid = $ret['pid'];
                    mll("Worker Exit, PID=" . $pid);
                    mll("Child num=" . $this->child);*/
                    $this->child--;
                }
        }
    }
    
    /**
     * nohup php yii message-queue/eat-sending >> nohup-eat-sending.txt 2>&1 &
     */
    public function actionEatSending()
    {
        pcntl_signal(SIGCHLD, [$this, 'signalHandler']);
        // 队列处理不超时 解决redis报错:read error on connection
        ini_set('default_socket_timeout', -1);

//        mll('init-time:' . time());
        while (true) {
            pcntl_signal_dispatch();
            if ($this->child < $this->maxProcesses) {
                $redis   = $this->redis('eat-sending');
                $dataPop = $redis->rpop(self::$sendingTask);
                $dataPop = json_decode($dataPop, true);
                if (!is_array($dataPop)) {
                    /*mll('sending empty pop');
                    mll($dataPop);
                    mll('current child:' . $this->child);
                    mll('now-time:' . time());*/
                    usleep(30000);
                    continue;
                }
                $process = new \swoole_process([$this, 'process']);
                /*mll('real arr');
                mll($dataPop);*/
                $dataPop['worker_type'] = 'eat-sending';
                if ($process->start()) {
                    $this->child++;
                    $process->write(json_encode($dataPop));
                } else {
                    $redis->rpush(self::$sendingTask, json_encode($dataPop));
                }
            }
        }
    }
    
    /**
     * nohup php yii message-queue/eat-cron >> nohup-eat-cron.txt 2>&1 &
     */
    public function actionEatCron()
    {
        pcntl_signal(SIGCHLD, [$this, 'signalHandler']);
        // 队列处理不超时 解决redis报错:read error on connection
        ini_set('default_socket_timeout', -1);
        
        while (true) {
            pcntl_signal_dispatch();
            if ($this->child < $this->maxProcesses) {
                $redis   = $this->redis('eat-cron');
                $dataPop = $redis->rpop(self::$cronTask);
                $dataPop = json_decode($dataPop, true);
                if (!is_array($dataPop)) {
                    usleep(30000);
                    continue;
                }
                $process                = new \swoole_process([$this, 'process']);
                $dataPop['worker_type'] = 'eat-cron';
                if ($process->start()) {
                    $this->child++;
                    $process->write(json_encode($dataPop));
                } else {
                    $redis->rpush(self::$cronTask, json_encode($dataPop));
                }
            }
        }
    }
    
    private function updateMission()
    {
        if ($missionData = $this->redis()->rpop(self::$missionTask)) {
            $missionDataArr = json_decode($missionData, true);
            $missionAtTime  = strtotime($missionDataArr['mission_at']);
            $nowTime        = time();
            
            if ($nowTime > $missionAtTime) {
                // 根据任务时间 检索完成状态 未完成标记成is_todo
                $missionMessage          = MessageUserRelation::findOne(['message_id' => $missionDataArr['message_id'], 'message_path_id' => $missionDataArr['message_path_id'], 'is_toto' => 0]);
                $missionMessage->is_todo = 1;
                $missionMessage->save();
            } else {
                $this->pushTasks([$missionDataArr], self::$missionTask);
            }
        } else {
            sleep(1);
        }
    }
    
    /**
     * 子进程将redis list中的数据吃掉
     *
     * @param array $post
     *
     * @return bool
     */
    private function eatMessage($post = [])
    {
        if (empty($post)) {
            return false;
        }
        if (isset($post['had_insert'])) {
            // 已经插入过了
            $cronAtTime = strtotime($post['cron_at']);
            $nowTime    = time();
            
            if ($nowTime < $cronAtTime) {
                // 没到定时发送时间不发送 插入定时队列
                $this->pushTasks([$post], self::$cronTask);
                
                return false;
            } else {
                $this->notifySwoole([$post]);
                
                return false;
            }
        }
        // 过model类型检测
        $post['title']   = (string)$post['title'];
        $post['content'] = (string)$post['content'];
        // socket暂且不处理推送时的排序问题 因为即时 所以推送一般直接是最新了
        $transaction = $this->db()->beginTransaction();
        try {
            $receiverUserIds           = $post['receiver_user_ids'];
            $post['receiver_user_ids'] = json_encode($post['receiver_user_ids'], JSON_NUMERIC_CHECK);
            $senderUid                 = $post['uid'];
            $post['sender_uid']        = $senderUid;
            
            $messageMod = new Message();
            $messageMod->load($post, '');
            
            $importantCoefficient         = $messageMod->getImportantCoefficient()->one();
            $messageMod->message_tag_id   = $importantCoefficient->message_tag_id;
            $isRedFlag                    = $importantCoefficient->message_tag_id == $this->app()->params['redFlagMessageTagId'];
            $isStick                      = $importantCoefficient->is_stick;
            $messageMod->sent_is_red_flag = $isRedFlag;
            
            $post['mission_at'] = isset($post['mission_at']) ? $post['mission_at'] : null;
            // 队列中的数据已经验证过了 此处不再验证 加快入库
            if ($messageMod->save(false)) {
                $fields                = ['message_id', 'receiver_uid', 'message_path_id', 'is_todo', 'is_red_flag'];
                $data                  = [];
                $swooleData            = [];
                $isToDo                = $post['mission_at'] ? 2 : 0;
                $isStarContactMessage  = 0;
                $messagePathIds        = $importantCoefficient->getNotificationWays()->select('message_path_id')->column();
                $receiverUser          = AuthAdmin::find()->select('employeeId263 as username, id')->where(['id' => $receiverUserIds])->asArray()->all();
                $receiverIdUsernameMap = array_column($receiverUser, 'username', 'id');
                
                foreach ($messagePathIds as $notificationPathId) {
                    foreach ($receiverUserIds as $receiverUserIdIndex => $receiverUserId) {
                        $values       = [$messageMod->attributes['id'], $receiverUserId, $notificationPathId, $isToDo, $isRedFlag];
                        $combinedData = array_combine($fields, $values);
                        if (AuthAdmin::findIdentity($receiverUserId)->getStarContacts()->where(['star_uid' => $senderUid])->exists()) {
                            $isStarContactMessage = 1;
                        }
                        $combinedData += ['is_star_contact_message' => $isStarContactMessage];
                        if ($post['mission_at']) {
                            $combinedData += ['mission_at' => $post['mission_at']];
                        }
                        if ($isStick) {
                            $combinedData += ['is_stick' => 1];
                        }
                        $data[]     = $combinedData;
                        $socketData = ['message_id' => $messageMod->id, 'receiver_uid' => $receiverUserId, 'message_path_id' => $notificationPathId, 'username' => $receiverIdUsernameMap[$receiverUserId]];
                        if (!isset($post['cron_at'])) {
                            $swooleData[] = $socketData;
                        } else {
                            $cronAtTime = strtotime($post['cron_at']);
                            $nowTime    = time();
                            
                            if ($nowTime < $cronAtTime) {
                                // socket 没到定时发送时间不发送 插入定时队列
                                $this->pushTasks([array_merge($socketData, ['had_insert' => true, 'cron_at' => $post['cron_at']])], self::$cronTask);
                            } else {
                                $swooleData[] = $socketData;
                            }
                        }
                    }
                }
                
                if (MessageUserRelation::addAll($data)) {
                    $transaction->commit();
                    $this->notifySwoole($swooleData);
                    
                    return true;
                } else {
                    $transaction->rollBack();
                    
                    $this->pushFailTask($post);
                    
                    return false;
                }
            } else {
                $transaction->rollBack();
                
                $this->pushFailTask($post);
                
                return false;
            }
        } catch (\Exception $exception) {
            $post['exception-message'] = $exception->getMessage();
            $post['exception-file']    = $exception->getFile();
            $post['exception-line']    = $exception->getLine();
            $post['exception-at']      = date('Y-m-d H:i:s');
            
            $this->pushFailTask($post);
            
            $transaction->rollBack();
            
            return false;
        }
    }
    
    /**
     * 将消息从左边推进消息队列
     *
     * @param $post
     *
     * @return mixed
     */
    public function pushSendingTask($post)
    {
        return $this->redis()->lpush(self::$sendingTask, json_encode($post));
    }
    
    /**
     * @param $posts [[], []...] 任务二维数组
     * @param $type  string redisKey
     */
    public function pushTasks($posts, $type)
    {
        foreach ($posts as $post) {
            $this->redis()->lpush($type, json_encode($post));
        }
    }
    
    /**
     * 在吃消息的子进程中将消息推给swoole队列
     *
     * @param $dataList
     *
     * @return bool
     */
    private function notifySwoole($dataList)
    {
        // ['message_id', 'sender_uid', 'receiver_uid', 'message_path_id', 'is_todo', 'is_red_flag', 'username'];
        foreach ($dataList as $data) {
            $this->getChildRedis()->lpush(self::$swooleTask, json_encode($data));
        }
        
        return true;
    }
    
    /**
     * 左推失败消息进list队列
     * failTask 在子进程中处理
     *
     * @param $post
     *
     * @return bool|int
     */
    private function pushFailTask($post)
    {
        return $this->getChildRedis()->lpush(self::$failTask, json_encode($post));
    }
}

function mll($data)
{
    if (false !== strpos(php_uname(), 'DESKTOP-2JEJJIV')) {
        print_r($data);
        echo PHP_EOL;
    }
}
