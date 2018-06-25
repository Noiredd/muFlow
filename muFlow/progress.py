import time

PRINT_FLAG = True
VT100_FLAG = True
VT100_DELETE_LINE = '\x1b[1A' + '\x1b[2K' + '\x1b[1A'

class ParallelReporter(object):
  #a callable object that keeps track of the number of times it has been called
  #and, given the total number of times it is going to be called, prints some
  #progress reports every second
  def __init__(self, text):
    self.message = text
    self.isSet   = False
    self.total   = -1
    self.count   = 0
  
  def setup(self, n):
    self.total = n
    self.count = 0
    self.start = time.time()
    self.last  = self.start
    self.isSet = True
  
  def __call__(self, total_count):
    global PRINT_FLAG
    global VT100_FLAG
    global VT100_DELETE_LINE
    #the assumptions are that total_count does not change between calls,
    #and the first call initializes the object
    #the latter is due to not knowing the total_count at construction
    if not self.isSet:
      self.setup(total_count)
    #keep track of the number of processed items
    self.count += 1
    #check whether a full second elapsed since the last call
    if time.time() - self.last > 1.0:
      #calculate % of work done and print
      percent = 1.0 * self.count / self.total
      if PRINT_FLAG:
        if VT100_FLAG: print(VT100_DELETE_LINE)
        print(self.message + ', {:3.0f}% done '.format(100*percent))
      #log the last call time
      self.last = time.time()

class SerialReporter(object):
  #keeps track of time, allowing measuring time taken by a sequence of tasks
  #to use:
  # start(what) to start keeping track of a task (prints)
  # stop() to stop and report that task (prints)
  # total() to return total time since creation (optionally prints)
  def __init__(self):
    self.task_text = ''
    self.task_time = None
    self.birth = time.time()
  
  def start(self, task_text):
    global PRINT_FLAG
    self.task_text = task_text
    self.task_time = time.time()
    if PRINT_FLAG:
      print(self.task_text)
  
  def __print_event(self, text, secs):
    print(text + '  ({:.1f}s)'.format(secs))
  
  def stop(self):
    global PRINT_FLAG
    global VT100_FLAG
    global VT100_DELETE_LINE
    if self.task_time is not None:
      taken = time.time() - self.task_time
      if PRINT_FLAG:
        if VT100_FLAG:
          print(VT100_DELETE_LINE)
        self.__print_event(self.task_text, taken)
      #self.task_time = None
      return taken

  def total(self, message=None):
    global PRINT_FLAG
    taken = time.time() - self.birth
    if message is not None and PRINT_FLAG:
      self.__print_event(message, taken)
    return taken

def useVT100(flag):
  global VT100_FLAG
  VT100_FLAG = True if flag else False

def usePrint(flag):
  global PRINT_FLAG
  PRINT_FLAG = True if flag else False
