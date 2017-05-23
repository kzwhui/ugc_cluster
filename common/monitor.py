#!/usr/local/bin/python
#encoding=utf8
import boss_api
import log 

#上报monitor
class MonitorReporter(object):
    def __init__(self):
        self.monitorIdDict = {
                "ugc_download_num":424124,
                "ugc_download_suc":424125,
                "ugc_download_fail":424126,
                "ugc_upload_num":424121,
                "ugc_upload_suc":424122,
                "ugc_upload_fail":424123,
                "ugc_callback_num":424127,
                "ugc_callback_suc":424128,
                "ugc_callback_fail":424129,
                "ugc_update_num":424582,
                "ugc_update_suc":424583,
                "ugc_update_fail":424584,
                "ugc_download_new_num":424964,
                "ugc_download_handling_num":424965,
                "ugc_upload_new_num":424966,
                "ugc_callback_new_num":424967,
                "ugc_callback_handling_num":424968,
                }
    
    def attr_add(self, attr_id, num = 1):
        ret, res = boss_api.attr_add(attr_id, num)
        if ret:
            log.error("attr_add fail, attr_id=%s, num=%s, ret=%d, res=%s" % (attr_id, num, ret, res))
    
    def attr_set(self, attr_id, num = 0):
        ret, res = boss_api.attr_set(attr_id, num)
        if ret:
            log.error("attr_set fail, attr_id=%s, num=%s, ret=%d, res=%s" % (attr_id, num, ret, res))
    
    def report_download_all(self):
        id = self.monitorIdDict["ugc_download_num"]
        self.attr_add(id)
    
    def report_download_suc(self):
        self.report_download_all()
        id = self.monitorIdDict["ugc_download_suc"]
        self.attr_add(id)

    def report_download_fail(self):
        self.report_download_all()
        id = self.monitorIdDict["ugc_download_fail"]
        self.attr_add(id)

    def report_upload_all(self):
        id = self.monitorIdDict["ugc_upload_num"]
        self.attr_add(id)

    def report_upload_suc(self):
        self.report_upload_all()
        id = self.monitorIdDict["ugc_upload_suc"]
        self.attr_add(id)

    def report_upload_fail(self):
        self.report_upload_all()
        id = self.monitorIdDict["ugc_upload_fail"]
        self.attr_add(id)
    
    def report_callback_all(self):
        id = self.monitorIdDict["ugc_callback_num"]
        self.attr_add(id)

    def report_callback_suc(self):
        self.report_callback_all()
        id = self.monitorIdDict["ugc_callback_suc"]
        self.attr_add(id)

    def report_callback_fail(self):
        self.report_callback_all()
        id = self.monitorIdDict["ugc_callback_fail"]
        self.attr_add(id)

    def report_update_all(self):
        id = self.monitorIdDict["ugc_update_num"]
        self.attr_add(id)

    def report_update_suc(self):
        self.report_update_all()
        id = self.monitorIdDict["ugc_update_suc"]
        self.attr_add(id)

    def report_update_fail(self):
        self.report_update_all()
        id = self.monitorIdDict["ugc_update_fail"]
        self.attr_add(id)

    def report_download_new_num(self,num=0):
        id = self.monitorIdDict["ugc_download_new_num"]
        self.attr_set(id,num)

    def report_download_handling_num(self,num=0):
        id = self.monitorIdDict["ugc_download_handling_num"]
        self.attr_set(id,num)

    def report_upload_new_num(self,num=0):
        id = self.monitorIdDict["ugc_upload_new_num"]
        self.attr_set(id,num)

    def report_callback_new_num(self,num=0):
        id = self.monitorIdDict["ugc_callback_new_num"]
        self.attr_set(id,num)

    def report_callback_handling_num(self,num=0):
        id = self.monitorIdDict["ugc_callback_handling_num"]
        self.attr_set(id,num)

if __name__ == "__main__":
    monitor = MonitorReporter()
    #monitor.report_download_all()
    #monitor.report_download_handling_num(1)
