from logging.handlers import TimedRotatingFileHandler
from datetime import datetime
import os
import shutil
import time


class DailyRotatingFileHandler(TimedRotatingFileHandler):
    def __init__(self, base_filename, when='midnight', interval=1, backupCount=7, encoding=None, delay=False, utc=False,
                 atTime=None):
        self.base_filename = base_filename
        base_dir = os.path.dirname(base_filename)
        if base_dir and not os.path.exists(base_dir):
            os.makedirs(base_dir)
        self.archive_folder = os.path.join(os.path.dirname(base_filename), "archive")
        if not os.path.exists(self.archive_folder):
            os.makedirs(self.archive_folder)
        super().__init__(self._current_filename(), when, interval, backupCount, encoding, delay, utc, atTime)

    def _current_filename(self):
        current_date = datetime.now().strftime("%Y-%m-%d")
        filename = f"{self.base_filename}_{current_date}.log"
        return filename

    def doRollover(self):
        if self.stream:
            self.stream.close()
            self.stream = None
        current_time = int(time.time())
        dst_filename = self._current_filename()

        if os.path.exists(self.baseFilename):
            src = self.baseFilename
            dst = os.path.join(self.archive_folder, os.path.basename(dst_filename))
            if os.path.exists(dst):
                os.remove(dst)
            shutil.move(src, dst)

        if self.backupCount > 0:
            for s in self.getFilesToDelete():
                os.remove(s)

        self.mode = 'w'
        self.stream = self._open()

        current_time = int(time.time())
        new_rollover_at = self.computeRollover(current_time)
        while new_rollover_at <= current_time:
            new_rollover_at = new_rollover_at + self.interval
        self.rolloverAt = new_rollover_at

    def getFilesToDelete(self):
        dir_name, base_name = os.path.split(self.baseFilename)
        file_names = os.listdir(self.archive_folder)
        result = []
        prefix = base_name.split('_')[0]  # Get the prefix of the log file
        p_len = len(prefix)
        for file_name in file_names:
            if file_name[:p_len] == prefix and file_name.endswith(".log"):
                result.append(os.path.join(self.archive_folder, file_name))
        if len(result) < self.backupCount:
            result = []
        else:
            result.sort()
            result = result[:len(result) - self.backupCount]
        return result