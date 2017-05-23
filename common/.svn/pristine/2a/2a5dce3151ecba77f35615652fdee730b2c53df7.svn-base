#encoding=gbk
import traceback
import sys
import thread
import os
import re
import time
import struct
import logging

def create_logger(logfilename , logName="") :
    """创建日志对象"""
    current_process = os.path.basename(sys.argv[0])
    current_process = current_process[0:current_process.rfind(".py")]
    import logging,logging.handlers
    logger = logging.getLogger(logName)
    infohdlr = logging.handlers.RotatingFileHandler(logfilename+current_process+'.info.log', maxBytes=100*1000*1000, backupCount=10)
    infohdlr.setLevel(logging.INFO)
    #detail
    #debughdlr = logging.handlers.RotatingFileHandler(logfilename+current_process+'.debug.log', maxBytes=500*1000*1000, backupCount=10)
    #debughdlr.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s %(levelname)6s  %(threadName)-12s %(filename)-10s  %(lineno)4d:%(funcName)16s- %(message)s')

    infohdlr.setFormatter(formatter)
    #debughdlr.setFormatter(formatter)

    logger.addHandler(infohdlr)
    #logger.addHandler(debughdlr)

    logger.setLevel(logging.INFO)
    return logger

logger = create_logger('../log/')
def alarm(alarmMsg, dead=False, alarm_level=0):
    global g_logger
    g_logger.critical('[ALARM]'+str(alarmMsg))
    print '[ALARM]'+str(alarmMsg)
    g_logger.critical(traceback.format_exc())
    if dead and False:
        thread.interrupt_main()
        os.exit(1)
