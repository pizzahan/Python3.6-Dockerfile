#!/usr/local/bin/python3
# coding=utf-8

from kafka import KafkaProducer
import time
import traceback
import logging


def BKDRHash(key):
    seed = 131  # 31 131 1313 13131 131313 etc..
    hash = 0
    for i in range(len(key)):
        hash = (hash * seed) + ord(key[i])
    return hash & 0x7FFFFFFF


def DJBHash(key):
    hash = 5381
    for i in range(len(key)):
        hash = ((hash << 5) + hash) + ord(key[i])
    return hash & 0x7FFFFFFF


class KafkaSender:
    __stop = False
    __lastSendTime = 0

    def __init__(self, kafkaHost, topic, topicNum, keyField, fileReader, sendTimeInterval, sendNumInterval, maxSize,
                 passCondition, ignore, reconnectinterval, sleepPer5sec, username, passward, requestSize, retries,
                 sendTime, sendSize, minHash, maxHash):
        self.__hosts = kafkaHost
        self.__topic = topic
        self.__topicNum = topicNum
        self.__keyField = keyField
        self.__fileReader = fileReader
        self.__sendTimeInterval = sendTimeInterval
        self.__sendNumInterval = sendNumInterval
        self.__maxSize = maxSize
        self.__requestSize = requestSize
        self.__passCondition = passCondition
        self.ignoreField = ignore
        self.__lastReconnectTime = time.time()
        self.__lastSleepTime = time.time()
        self.__lastReadFileTime = time.time()
        self.__reconnectinterval = reconnectinterval
        self.__sleepPer5sec = sleepPer5sec
        self.__producer = None
        self.__username = username
        self.__passward = passward
        self.__retries = retries
        self.__sendSize = sendSize
        self.__sendTime = sendTime
        self.__minHash = minHash
        self.__maxHash = maxHash
        self.__futures = []
        self.__msgCnt = 0  # 计数
        self.fields = []
        self.__topicOffsetMap = {}
        self.__msgs = []
        self.__connect()
        self.__line = ""
        self.__lastLogTime = time.time()
        self.__pkgSize = 0
        return

    def __connect(self):
        try:
            if self.__producer is not None:
                self.__producer.flush(10)
                self.__producer.close(5)
        except Exception as e:
            logging.warning("close producer failed.")
            logging.warning(e)
        try:
            hosts = self.__hosts.split(",")
            if len(self.__username) > 0:
                self.__producer = KafkaProducer(
                    bootstrap_servers=hosts,
                    compression_type='snappy',
                    security_protocol='SASL_PLAINTEXT',
                    sasl_mechanism='PLAIN',
                    sasl_plain_username=self.__username,
                    sasl_plain_password=self.__passward,
                    max_request_size=self.__requestSize,
                    batch_size=self.__sendSize,
                    linger_ms=self.__sendTime,
                    retries=self.__retries
                )
            else:
                self.__producer = KafkaProducer(
                    bootstrap_servers=hosts,
                    compression_type='snappy',
                    max_request_size=self.__requestSize,
                    batch_size=self.__sendSize,
                    linger_ms=self.__sendTime,
                    retries=self.__retries)
            logging.info("connect kafka ok")
        except Exception as e:
            logging.warning("connect to kafka[" + self.__hosts + "] failed")
            logging.warning(e)
            return False
        return True

    def __reconnect(self):
        conn = self.__connect()
        while not conn and not self.__stop:
            logging.warning("try to reconnect kafka")
            time.sleep(1)
            conn = self.__connect()
            continue

    def send(self):
        if self.__msgCnt == 0:
            self.__lastSendTime = time.time()
            logging.info("no msg need to be send at %.0f", self.__lastSendTime)
            return

        # 批量发送
        # logging.info("send [%d] msg", self.__msgCnt)
        for i in range(0, self.__msgCnt):
            while not self.__stop:
                try:
                    # logging.info("send [%d] key [%s]", i, self.__keys[i])
                    # 按照key做分布
                    future = self.__producer.send(self.__topic, value=self.__msgs[i])
                    self.__futures.append(future)
                    break
                except Exception as e:
                    logging.error(e)
                    logging.info("send exception[%s]" % traceback.format_exc())
                    self.__reconnect()

        # 退出
        if self.__stop:
            self.__producer.close()
            self.__fileReader.close()
            return
        # 立刻发送 timeout 100s
        # logging.info("flush msg[%d] size[%d]"%(self.__msgCnt, self.__pkgSize))
        self.__producer.flush(timeout=100)
        # 校验发送结果
        for i in range(0, self.__msgCnt):
            retry_times = 0
            while not self.__stop:
                try:
                    meta = self.__futures[i].get(timeout=100)
                    if meta.partition not in self.__topicOffsetMap:
                        self.__topicOffsetMap[meta.partition] = 0
                    self.__topicOffsetMap[meta.partition] = max(meta.offset, self.__topicOffsetMap[meta.partition])
                    break
                except UnicodeDecodeError:
                    logging.error("unicode decode error[%s]" % self.__msgs[i])
                    break
                except Exception as e:
                    logging.error(e)
                    logging.error("send future exception[%s]" % traceback.format_exc())
                    self.__reconnect()
                    for j in range(i, self.__msgCnt):
                        self.__futures[j] = self.__producer.send(self.__topic, value=self.__msgs[j])
                    self.__producer.flush()
                    retry_times += 1
                    if retry_times >= 3:
                        logging.error("max retry at idx[%d], total[%d]msgs, server exit." % (i, self.__msgCnt))
                        # 主动退出,等待拉起
                        self.__stop = True

        if not self.__stop:
            # 更新读取文件进度
            self.__fileReader.storeProgress()
            self.__log_msgnum()
        else:
            self.__producer.close()
            self.__fileReader.close()
            return
        self.__msgs = []
        self.__futures = []
        self.__msgCnt = 0
        self.__pkgSize = 0
        time.sleep(self.__sleepPer5sec)
        self.__lastSendTime = time.time()
        return

    def stop(self):
        self.__stop = True

    def run(self):
        while not self.__stop:
            now = time.time()
            # 　是否需要重连，每隔一段时间重连一次
            if now - self.__lastReconnectTime >= self.__reconnectinterval:
                self.__reconnect()
                self.__lastReconnectTime = now

            if now - self.__lastSendTime >= self.__sendTimeInterval or self.__msgCnt >= self.__sendNumInterval or self.__pkgSize >= self.__maxSize:
                self.send()

            # 获得完整一行(以\n结尾)
            while not self.__stop:
                l = self.__fileReader.getLine()
                self.__line = self.__line + l
                if len(self.__line) > 0 and self.__line[-1] == '\n':
                    self.__lastReadFileTime = time.time()
                    break
                if time.time() - self.__lastReadFileTime > 5:
                    self.__lastReadFileTime = time.time()
                    break
            # 空行返回
            if self.__line == "" or self.__line == "\n":
                logging.error(r"{0}行是空行".format(self.__msgCnt))
                continue
            # 半行 不解析不清空
            if len(self.__line) > 0 and self.__line[-1] != '\n':
                logging.error(r"未以\n结尾")
                continue
            # 解析并处理
            if self.__msgDeal():
                self.__msgCnt = self.__msgCnt + 1
                self.__pkgSize = self.__pkgSize + len(self.__line)
            # logging.info("msg[%d], size[%d]" % (self.__msgCnt, self.__pkgSize))
            # 清空以便读取下一行
            self.__line = ""

    def __log_msgnum(self):
        # topic | packageNum | messageNum | offsetMap
        logging.info("\t%s\t%d\t%d\t%s" % (self.__topic, self.__pkgSize, self.__msgCnt, str(self.__topicOffsetMap)))
        self.__topicOffsetMap.clear()
        self.__lastLogTime = time.time()

    def __msgDeal(self):
        try:
            self.fields = self.__line.split('\t')
            # 过滤
            if self.__passCondition != '':
                if not eval(self.__passCondition):
                    return False

            if len(self.fields) < self.__keyField or len(self.fields) < self.ignoreField:
                return False
            '''
            # hash过滤
            key = self.fields[self.__keyField - 1]
            hash_value = DJBHash(key) % 100
            if hash_value < self.__minHash or hash_value >= self.__maxHash:
                # logging.warn("hash[%d] not in [%d,%d)" % (hash_value, self.__minHash, self.__maxHash))
                return False
            '''
            # 替换无效大字段
            if self.ignoreField != 0:
                self.fields[self.ignoreField - 1] = "-"
            msg = "\t".join(self.fields).encode('utf-8')
            self.__msgs.append(msg)
            return True
        except Exception as e:
            logging.error(e)
            logging.error(traceback.format_exc())
            return False
