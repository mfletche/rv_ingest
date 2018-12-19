'''
Created on 18/12/2018

@author: mfletche
'''

''' Aliases are IPv6 addresses that refer to the same physical router.
'''
aliases = []

class SPF:
    
    def __init__(self):
        #self.interface    # IP address of interface
        #self.reboot_start    # Reboot window start
        #self.reboot_end    # Reboot window end
        #self.prefix        # Prefix
        #self.dist        # Distance annotation
        self.peers = {}
        #self.updates
        pass
        
    def set_interface(self, interface):
        self.interface = interface
        
    def set_reboot_window(self, start, end):
        self.reboot_start = start
        self.reboot_end = end