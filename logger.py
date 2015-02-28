from logging.handlers import TimedRotatingFileHandler, RotatingFileHandler
import multiprocessing, threading

import logging, sys, traceback, os


def init_loggers( file=None, verbose=False, level=logging.INFO, quiet=False, log_format='%(asctime)s %(process)d\t%(levelname)s\t%(message)s', backupCount=3, when='midnight', **kwargs ):
    """
    if file == None, then log to console
    """
    if verbose:
        level=logging.DEBUG
    if quiet:
        level=logging.WARN
        
    # check to make sure we can log to the file
    ch = None
    if file:
        try:
            with open( file, 'a+' ) as f:
                pass
        except Exception,e:
            raise e
        ch = MultiProcessingLogHandler( file, when=when, backupCount=backupCount )
        # ch = Dummy( file, when='midnight', backupCount=7  )
    else:
        ch = logging.StreamHandler()
    ch.setFormatter( logging.Formatter( log_format ) )
    LOG = logging.getLogger('')
    LOG.setLevel( level )
    LOG.addHandler(ch)
    # logging.error('logging file %s initiated at level %s (%s/%s) %s' % (file,level,verbose,quiet,logging.INFO))
    return LOG

class Dummy(logging.handlers.TimedRotatingFileHandler):
    pass

# also see http://bugs.python.org/file23830/log.patch
# class TimedCompressedRotatingFileHandler(TimedRotatingFileHandler):
#     """
#        Extended version of TimedRotatingFileHandler that compress logs on rollover.
#        by Angel Freire <cuerty at gmail dot com>
#     """
#     def doRollover(self):
#         """
#         do a rollover; in this case, a date/time stamp is appended to the filename
#         when the rollover happens.  However, you want the file to be named for the
#         start of the interval, not the current time.  If there is a backup count,
#         then we have to get a list of matching filenames, sort them and remove
#         the one with the oldest suffix.
# 
#         This method is a copy of the one in TimedRotatingFileHandler. Since it uses
#         
#         """
#         self.stream.close()
#         # get the time that this sequence started at and make it a TimeTuple
#         t = self.rolloverAt - self.interval
#         timeTuple = time.localtime(t)
#         dfn = self.baseFilename + "." + time.strftime(self.suffix, timeTuple)
#         if os.path.exists(dfn):
#             os.remove(dfn)
#         os.rename(self.baseFilename, dfn)
#         if self.backupCount > 0:
#             # find the oldest log file and delete it
#             s = glob.glob(self.baseFilename + ".20*")
#             if len(s) > self.backupCount:
#                 s.sort()
#                 os.remove(s[0])
#         #print "%s -> %s" % (self.baseFilename, dfn)
#         if self.encoding:
#             self.stream = codecs.open(self.baseFilename, 'w', self.encoding)
#         else:
#             self.stream = open(self.baseFilename, 'w')
#         self.rolloverAt = self.rolloverAt + self.interval
#         if os.path.exists(dfn + ".zip"):
#             os.remove(dfn + ".zip")
#         file = zipfile.ZipFile(dfn + ".zip", "w")
#         file.write(dfn, os.path.basename(dfn), zipfile.ZIP_DEFLATED)
#         file.close()
#         os.remove(dfn)


class MultiProcessingLogHandler(logging.Handler):
    def __init__(self, *args, **kwargs):
        logging.Handler.__init__(self)

        self._handler = TimedRotatingFileHandler(*args, **kwargs)
        self.queue = multiprocessing.Queue(-1)

        self.stream = self._handler.stream

        t = threading.Thread(target=self.receive)
        t.daemon = True
        t.start()

    def setFormatter(self, fmt):
        logging.Handler.setFormatter(self, fmt)
        self._handler.setFormatter(fmt)

    def receive(self):
        while True:
            try:
                record = self.queue.get()
                self._handler.emit(record)
            except (KeyboardInterrupt, SystemExit):
                raise
            except EOFError:
                break
            except Exception,e:
                print "ERROR! %s %s" % (type(e),e)
                traceback.print_exc(file=sys.stderr)

    def send(self, s):
        self.queue.put_nowait(s)

    def _format_record(self, record):
        # ensure that exc_info and args
        # have been stringified.  Removes any chance of
        # unpickleable things inside and possibly reduces
        # message size sent over the pipe
        if record.args:
            record.msg = record.msg % record.args
            record.args = None
        if record.exc_info:
            dummy = self.format(record)
            record.exc_info = None

        return record

    def emit(self, record):
        try:
            s = self._format_record(record)
            self.send(s)
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record)

    def close(self):
        self._handler.close()
        logging.Handler.close(self)
