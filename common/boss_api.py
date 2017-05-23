#!/usr/local/bin/python
#monitor
import commands

def attr_set(attr_id, num, ip = None):
    if ip == None:
        ip = ""
    cmd = "/usr/local/agenttools/agent/agentRepNum %s %s %s" % (ip, attr_id, num)
    ret, res = commands.getstatusoutput(cmd)
    #print "ret=%d, res=%s" % (ret, res)
    return ret, res
    #if ret:
    #    raise Exception("make md5 file fail, cmd=%s" % (cmd))

def attr_add(attr_id, num, ip = None):
    if ip == None:
        ip = ""
    cmd = "/usr/local/agenttools/agent/agentRepSum %s %s %s" % (ip, attr_id, num)
    ret, res = commands.getstatusoutput(cmd)
    #print "ret=%d, res=%s" % (ret, res)
    return ret, res
