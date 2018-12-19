'''
Created on 19/12/2018

@author: mfletche
'''

class TimeWindow:
    ''' A window of time defined by a start and an end.
    '''
    def __init__(self, start, end):
        if (start > end): raise ValueError
        self.start = start
        self.end = end
        
    def contains(self, time_window):
        ''' Checks whether this time window completely contains another time
        window. This will return True if the time windows are the same.
        @return: True if this TimeWindow contains the other time window
        completely. 
        '''
        assert(self.start <= self.end)
        assert(time_window.start <= time_window.end)
        
        return (self.start <= time_window.start
                and time_window.end <= self.end)   
    
    def duration(self):
        ''' Returns the duration of the time window as the difference between
        the start and end times.
        '''
        assert(self.start <= self.end)
        return self.end - self.start
    
    def overlaps(self, time_window):
        ''' Checks whether this time window overlaps with a different time
        window.
        @return: True if the windows overlap.
        '''
        assert(self.start <= self.end)
        assert(time_window.start <= time_window.end)
        
        return (self.end <= time_window.start
                or time_window.end <= self.start)