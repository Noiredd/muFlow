class Reporter(object):
  #a callable object that keeps track of the number of times it has been called
  #and, given the total number of times it is going to be called, every some
  #percent of the calls, prints some progress reports
  DELETE_LINE = '\x1b[1A' + '\x1b[2K' + '\x1b[1A'

  def __init__(self, text, percent_step):
    self.message = text
    self.p_step  = percent_step * 1.0
    self.isSetup = False
    self.total   = -1
    self.count   = 0
  
  def setup(self, n):
    self.total = n
    self.count = 0
    if self.p_step > 0 and self.p_step < 1.0:
      self.steps = [i*self.p_step for i in range(1, int(1.0 / self.p_step) + 1)]
      self.steps.append(99)  #something big so there's always at least 1 element
    else:
      self.steps = None
    self.isSetup = True
    print(self.message)
  
  def __call__(self, total_count):
    #the assumptions are that total_count does not change between calls,
    #and the first call initializes the object
    #the latter is due to not knowing the total_count at construction
    if not self.isSetup:
      self.setup(total_count)
    #keep track of the number of processed items
    self.count += 1
    #calculate % of work done and check if a message for this point was printed
    percent = 1.0 * self.count / self.total + 0.001 #against cases of 0.300..04
    while self.steps is not None and percent >= self.steps[0]:
      #while instead of if in case there are very few elements but requested
      #reporting frequency is relatively large (so we can skip over the steps)
      print(self.DELETE_LINE)
      print(self.message + ', {:3.0f}% done '.format(100*self.steps[0]))
      #pop the first item to mark this percentage as reported
      del self.steps[0]
