import time

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
    self.start = time.clock()
    self.last  = self.start
    self.isSet = True
  
  def __call__(self, total_count):
    #the assumptions are that total_count does not change between calls,
    #and the first call initializes the object
    #the latter is due to not knowing the total_count at construction
    if not self.isSet:
      self.setup(total_count)
    #keep track of the number of processed items
    self.count += 1
    #check whether a full second elapsed since the last call
    if time.clock() - self.last > 1.0:
      #calculate % of work done and print
      percent = 1.0 * self.count / self.total
      if VT100_FLAG: print(VT100_DELETE_LINE)
      print(self.message + ', {:3.0f}% done '.format(100*percent))
      #log the last call time
      self.last = time.clock()

class SerialReporter(object):
  #keeps track of time, allowing measuring time taken by a sequence of tasks
  #to use:
  # start(what) to start keeping track of a task (prints)
  # stop() to stop and report that task (prints)
  # total() to return total time since creation (optionally prints)
  def __init__(self):
    self.task_text = ''
    self.task_time = None
    self.birth = time.clock()
  
  def start(self, task_text):
    self.task_text = task_text
    self.task_time = time.clock()
    print(self.task_text)
  
  def print_event(self, text, secs):
    print(text + '  ({:.1f}s)'.format(secs))
  
  def stop(self):
    global VT100_FLAG
    global VT100_DELETE_LINE
    if self.task_time is not None:
      taken = time.clock() - self.task_time
      if VT100_FLAG:
        print(VT100_DELETE_LINE)
      self.print_event(self.task_text, taken)
      self.task_time = None

  def total(self, message=None):
    taken = time.clock() - self.birth
    if message is not None:
      self.print_event(message, taken)
    return taken

def useVT100(flag):
  global VT100_FLAG
  VT100_FLAG = True if flag else False
