#!/usr/local/bin/python3
# coding=utf-8
import os
import logging
import datetime
import time


class FileReader:
    __curFileInode = 0
    __curFileTimes = ""
    __offset = 0

    def __init__(self, progFile, fileDir, filePrefix, fileSuffix, timeFmt, rollMinutes):
        self.__progFile = progFile
        self.__fileDir = fileDir
        self.__filePrefix = filePrefix
        self.__fileSuffix = fileSuffix
        self.__timeFmt = timeFmt
        self.__rollMinutes = rollMinutes
        self.__curFd = None
        return

    def __tryNextFile(self):
        if os.path.getsize(self.__fileDir + "/" + self.__filePrefix + self.__curFileTimes + self.__fileSuffix) \
                > self.__curFd.tell():
            return

        nextTimes = self.__getNextTimes(self.__curFileTimes)
        nextFile = self.__fileDir + "/" + self.__filePrefix + nextTimes + self.__fileSuffix

        while not os.path.isfile(nextFile):
            nextTimes = self.__getNextTimes(nextTimes)
            if nextTimes == "":
                break
            nextFile = self.__fileDir + "/" + self.__filePrefix + nextTimes + self.__fileSuffix

        nextFile = self.__fileDir + "/" + self.__filePrefix + nextTimes + self.__fileSuffix
        if not os.path.isfile(nextFile):
            return

        if os.stat(nextFile).st_ino == self.__curFileInode:
            return

        self.__curFd.close()
        logging.info("close " + self.__fileDir + "/" + self.__filePrefix + self.__curFileTimes + self.__fileSuffix)
        self.__offset = 0
        if not self.open(nextTimes):
            raise Exception
        return

    def __getNextTimes(self, times):
        if times == "":
            return ""

        timeArr = times.split("/")
        times = timeArr[len(timeArr) - 1]
        fmtArr = self.__timeFmt.split("/")
        if len(fmtArr) >= 0:
            timeFmt = fmtArr[len(timeArr) - 1]
        else:
            timeFmt = "%Y-%m-%d-%H"
        nextTime = datetime.datetime.strptime(times, timeFmt) + datetime.timedelta(minutes=self.__rollMinutes)

        d = datetime.datetime.now()
        if nextTime - d > datetime.timedelta(minutes=1):
            time.sleep(0.1)
            return ""
        logging.info("next times: " + times)
        return datetime.datetime.strftime(nextTime, self.__timeFmt)

    def __getProgress(self):
        fd = open(self.__progFile, "r")
        line = fd.readline()
        l = line.split('\t')
        if len(l) != 3:
            fd.close()
            raise Exception
        self.__curFileInode = int(l[0])
        self.__curFileTimes = l[1]
        self.__offset = int(l[2])
        logging.info("get [%d][%s][%d] from [%s]" % (self.__curFileInode,
                                                     self.__curFileTimes, self.__offset, self.__progFile))
        fd.close()

    def open(self, manualTimes):
        if manualTimes == "" and self.__curFileInode == 0:
            try:
                self.__getProgress()
                manualTimes = self.__curFileTimes
            except Exception as e:
                logging.error("progFile [" + self.__progFile + "] parse failed")
                logging.error(e)
                return False

        filePath = self.__fileDir + "/" + self.__filePrefix + manualTimes + self.__fileSuffix
        try:
            inode = os.stat(filePath).st_ino
            fd = open(filePath, "r")
            # 防止open时文件变化,记录了不正确的inode,主要应对ad_server的日志命名规则
            if os.stat(filePath).st_ino != inode:
                fd.close()
                inode = os.stat(filePath).st_ino
                fd = open(filePath, "r")
            self.__curFd = fd
            self.__curFd.seek(self.__offset)
            self.__curFileTimes = manualTimes
            self.__curFileInode = inode
        except Exception as e:
            logging.error(e)
            logging.error("open " + filePath + " or seek to [" + str(self.__offset) + "] get exception")
            return False

        logging.info("open " + filePath + " and seek to [" + str(self.__offset) + "] success")
        return True

    def getLine(self):
        line = self.__curFd.readline()
        if line == "":
            time.sleep(10)
            self.__tryNextFile()
        return line

    def storeProgress(self):
        offset = self.__curFd.tell()
        fd = open(self.__progFile, "w")
        fd.write(str(self.__curFileInode) + "\t" + self.__curFileTimes + "\t" + str(offset))
        fd.close()
        os.system('cp ' + self.__progFile + ' ' + self.__progFile + ".bak")
        return

    def close(self):
        self.__curFd.close()
