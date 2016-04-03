"""Alarm event with GPS coordinates and a flag whether it was a night time alarm."""
class Alarm:
    def __init__(self, x, y, is_night_time):
        self.x = x
        self.y = y
        self.is_night_time = is_night_time

    
"""Cell information - alarm counts and data to calculate average cell coordinates."""
class Cell:
    def __init__(self, alarm):
        self.sum_x = alarm.x
        self.sum_y = alarm.y
        self.count = 1
        self.night_count = 1 if alarm.is_night_time else 0
        
    def add_info(self, alarm):
        self.sum_x += alarm.x
        self.sum_y += alarm.y
        self.count += 1
        self.night_count += 1 if alarm.is_night_time else 0
        
    def avg_coords(self):
        return (self.sum_x // self.count, self.sum_y // self.count, self.count, self.night_count)
    